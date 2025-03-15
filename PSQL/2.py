from sqlalchemy import create_engine, MetaData, Table, Column, Integer, VARCHAR, TIMESTAMP, func, text
import datetime

engine = create_engine('postgresql://dev_user:12345@localhost/python_db')

metadata = MetaData()

users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('email', VARCHAR(150), nullable=False),
    Column('created_at', TIMESTAMP, default=func.now(), nullable=False)
)

metadata.create_all(engine)

users_data = [
    ("user1@example.com",),
    ("user2@example.com",),
    ("user3@example.com",),
]

with engine.connect() as conn:
    # Используем execute и передаем данные построчно
    for email, in users_data:
        conn.execute(
            text('INSERT INTO users (email, created_at) VALUES (:email, :created_at)'),
            {'email': email, 'created_at': datetime.datetime.now()}
        )
