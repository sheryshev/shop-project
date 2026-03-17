from flask import Flask, render_template, request, flash
from flask_bootstrap import Bootstrap
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from kafka import KafkaProducer, KafkaConsumer
import threading
import json
import logging
import os



app = Flask(__name__)
app.secret_key = 'your_secret_key'
Bootstrap(app)

# --- Настройка базы данных SQLite ---
engine = create_engine('sqlite:///store.db', echo=False, future=True)

# --- Настройки Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
#KAFKA_BOOTSTRAP_SERVERS = ['junction.proxy.rlwy.net:36612']

#KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

kafka_messages_store = {}

def consume_kafka_messages(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='flask-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    kafka_messages_store[topic] = []
    for message in consumer:
        kafka_messages_store[topic].append(message.value)
        if len(kafka_messages_store[topic]) > 50:
            kafka_messages_store[topic].pop(0)
            
import json
import logging
from kafka import KafkaConsumer

def kafka_consumer_worker():
    print("--- [Kafka Worker] Starting ---")
    
    try:
        # Инициализация консьюмера
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='orders-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            # Добавляем таймаут на запрос метаданных, чтобы не виснуть вечно
            request_timeout_ms=30000, 
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Проверка физического соединения
        if consumer.bootstrap_connected():
            print(f"--- [Kafka Worker] Connected to {KAFKA_BOOTSTRAP_SERVERS} ---")
            print(f"--- [Kafka Worker] Subscribed to topics: {consumer.topics()} ---")
        else:
            print("!!! [Kafka Worker] CRITICAL: Could not connect to brokers !!!")
            return
        consumer.poll(timeout_ms=1000)
        # Основной цикл обработки
        for message in consumer:
            order_data = message.value
            print(f"--- [Kafka Worker] Received order: {order_data} ---")
            
            try:
                # Работа с БД внутри контекста Flask
                with app.app_context():
                    order = Order(
                        user_id=order_data.get('user_id'),
                        date=order_data.get('date'),
                        status=order_data.get('status')
                    )
                    db.session.add(order)
                    db.session.commit()
                    print(f"--- [Kafka Worker] Order saved with ID: {order.id} ---")
            
            except Exception as db_err:
                print(f"!!! [Kafka Worker] Database Error: {db_err} !!!")
                db.session.rollback() # Откатываем сессию при ошибке

    except Exception as kafka_err:
        print(f"!!! [Kafka Worker] Connection/Logic Error: {kafka_err} !!!")
    
    finally:
        print("--- [Kafka Worker] Stopped ---")


# --- Роуты ---

@app.route('/')
def index():
    sql = text("SELECT id, user_id, date, status FROM orders ORDER BY date DESC")
    with engine.connect() as conn:
        result = conn.execute(sql)
        columns = result.keys()
        orders = [dict(zip(columns, row)) for row in result]
    return render_template('base.html', orders=orders)

@app.route('/db', methods=['GET', 'POST'])
def db_view():
    results = None
    columns = None
    if request.method == 'POST':
        sql_query = request.form.get('sql_query')
        if sql_query:
            try:
                if not sql_query.strip().lower().startswith('select'):
                    flash('Разрешены только SELECT-запросы для безопасности.', 'warning')
                else:
                    with engine.connect() as conn:
                        result_proxy = conn.execute(text(sql_query))
                        columns = result_proxy.keys()
                        results = [dict(zip(columns, row)) for row in result_proxy]

            except SQLAlchemyError as e:
                flash(f'Ошибка выполнения запроса: {str(e)}', 'danger')
        else:
            flash('Введите SQL-запрос.', 'warning')
    return render_template('db_view.html', results=results, columns=columns)

@app.route('/kafka', methods=['GET', 'POST'])
def kafka_view():
    kafka_messages = None
    current_topic = None
    if request.method == 'POST':
        topic = request.form.get('kafka_topic')
        message = request.form.get('kafka_message')
        if topic and message:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: v.encode('utf-8')
                )
                producer.send(topic, message)
                producer.flush()
                flash(f'Сообщение отправлено в топик "{topic}".', 'success')

                if topic not in kafka_messages_store:
                    thread = threading.Thread(target=consume_kafka_messages, args=(topic,), daemon=True)
                    thread.start()

                current_topic = topic
                kafka_messages = kafka_messages_store.get(topic, [])
            except Exception as e:
                flash(f'Ошибка Kafka: {str(e)}', 'danger')
        else:
            flash('Введите топик и сообщение.', 'warning')
    else:
        if kafka_messages_store:
            current_topic = list(kafka_messages_store.keys())[-1]
            kafka_messages = kafka_messages_store[current_topic]

    return render_template('kafka.html', kafka_messages=kafka_messages, current_topic=current_topic)

@app.route('/documentation')
def documentation():
    return render_template('documentation.html')

if __name__ == '__main__':
    # Запускаем Kafka слушателя в отдельном потоке
    consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=False)
    consumer_thread.start()

    # Запускаем Flask сервер
    app.run(host='0.0.0.0', port=5000, debug=False)
