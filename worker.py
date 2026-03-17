import os
import json
import time
import logging
from kafka import KafkaConsumer
from sqlalchemy.exc import SQLAlchemyError

# Импортируем app, db и модель Order из вашего основного файла app.py
from app import app, db, Order

# Настройки Kafka из переменных окружения Railway
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def run_worker():
    print("--- [Worker] Инициализация консьюмера... ---")
    
    try:
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='orders-db-worker-group', # Уникальное имя группы
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            # Важно: используем исправленную библиотеку kafka-python-ng
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"--- [Worker] Подключено к: {KAFKA_BOOTSTRAP_SERVERS} ---")
        
        # Основной цикл вычитки сообщений
        for message in consumer:
            order_data = message.value
            print(f"--- [Worker] Получен заказ: {order_data} ---")
            
            # Работаем с БД через контекст Flask приложения
            try:
                with app.app_context():
                    new_order = Order(
                        user_id=str(order_data.get('user_id')),
                        date=str(order_data.get('date')),
                        status=str(order_data.get('status', 'new'))
                    )
                    db.session.add(new_order)
                    db.session.commit()
                    print(f"--- [Worker] Успешно сохранено в БД. ID: {new_order.id} ---")
            
            except SQLAlchemyError as db_err:
                print(f"!!! [Worker] Ошибка базы данных: {db_err} !!!")
                with app.app_context():
                    db.session.rollback()
            except Exception as e:
                print(f"!!! [Worker] Ошибка обработки данных: {e} !!!")

    except Exception as kafka_err:
        print(f"!!! [Worker] Критическая ошибка Kafka: {kafka_err} !!!")
        # Если упало — ждем 10 сек и выходим (Railway перезапустит процесс сам)
        time.sleep(10)

if __name__ == "__main__":
    run_worker()
