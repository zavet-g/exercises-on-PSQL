import psycopg2
import json

DB_CONFIG = {
    "dbname": "python_db",
    "user": "dev_user",
    "password": "12345"
}


def add_column():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as  cursor:
                cursor.execute('''
                ALTER TABLE blog_posts ADD COLUMN search_vector TSVECTOR;
                ''')
    except psycopg2.Error as e:
        print(f"Ошибка при создании колонки для поискового вектора: {e}")

def fill_data():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as  cursor:
                cursor.execute('''
                UPDATE blog_posts 
                SET search_vector = 
                    to_tsvector('russian', title) ||
                    to_tsvector('russian', content); 
                ''')
                conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при заполнении данными: {e}")

add_column()
fill_data()