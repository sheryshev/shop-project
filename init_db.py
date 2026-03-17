from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float

engine = create_engine('sqlite:///store.db', echo=True, future=True)
metadata = MetaData()

products = Table(
    'products', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('price', Float, nullable=False),
    Column('stock', Integer, nullable=False)
)

metadata.create_all(engine)

# Добавим несколько товаров
with engine.connect() as conn:
    conn.execute(products.insert(), [
        {'name': 'Ноутбук', 'price': 75000.0, 'stock': 10},
        {'name': 'Смартфон', 'price': 35000.0, 'stock': 25},
        {'name': 'Наушники', 'price': 5000.0, 'stock': 50},
    ])
    conn.commit()

print("База данных и таблицы созданы, данные добавлены.")
