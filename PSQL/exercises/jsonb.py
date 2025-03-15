import psycopg2
import json

DB_CONFIG = {
    "dbname": "python_db",
    "user": "dev_user",
    "password": "12345"
}

def create_db():
    """Создает таблицу, если она не существует."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS blog_posts(
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(200),
                        content TEXT,
                        meta JSONB 
                    );
                ''')
                conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при создании таблицы: {e}")
        conn.rollback()  # Откат изменений, если ошибка

def insert_data_in_db():
    """Добавляет тестовые данные в таблицу."""
    data = [
        ("Первая запись", "Это контент первой записи в блоге.", {"views": 120, "tags": ["Python", "Flask"], "author": "admin"}),
        ("Как использовать SQLAlchemy", "Разбираем основы SQLAlchemy с примерами.", {"views": 95, "tags": ["SQLAlchemy", "ORM"], "author": "editor"}),
        ("Асинхронные запросы в FastAPI", "Погружаемся в асинхронные запросы и PostgreSQL.", {"views": 250, "tags": ["FastAPI", "async", "PostgreSQL"], "author": "admin"}),
        ("Alembic: миграции в SQLAlchemy", "Рассмотрим основные команды Alembic.", {"views": 180, "tags": ["Alembic", "migrations"], "author": "contributor"}),
        ("Flask и JWT", "Настраиваем аутентификацию с помощью JWT.", {"views": 300, "tags": ["Flask", "JWT", "auth"], "author": "admin"})
    ]

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                for title, content, meta in data:
                    cursor.execute(
                        "INSERT INTO blog_posts (title, content, meta) VALUES (%s, %s, %s::jsonb);",
                        (title, content, json.dumps(meta))
                    )
                conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при вставке данных: {e}")
        conn.rollback()  # Откат изменений, если ошибка

def select_py_posts():
    """Выбирает все посты, у которых в meta->'tags' есть 'Python'."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(''' 
                    SELECT * FROM blog_posts WHERE meta->'tags' @> '["Python"]';
                ''')
                py_posts = cursor.fetchall()
                print(py_posts)
    except psycopg2.Error as e:
        print(f"Ошибка при выборке данных: {e}")
        conn.rollback()  # Откат изменений, если ошибка

# Запускаем функции
create_db()
insert_data_in_db()
select_py_posts()
