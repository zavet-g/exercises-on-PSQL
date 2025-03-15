import psycopg2
import json

DB_CONFIG = {
    "dbname": "python_db",
    "user": "dev_user",
    "password": "12345"
}


def create_gin():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                CREATE INDEX idx_blog_posts_meta_gin ON blog_posts USING gin (meta);
                ''')
    except psycopg2.Error as e:
        print(f"Ошибка при создании GIN-индекса: {e}")

def analyze_request():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                EXPLAIN ANALYZE SELECT * FROM blog_posts WHERE meta @> '{"tags": ["python"]}';
                ''')
                result = cursor.fetchall()
                for row in result:
                    print(row)
    except psycopg2.Error as e:
        print(f"Ошибка при анализе скорости запроса GIN-индекса: {e}")


create_gin()
analyze_request()