import psycopg2
import json

DB_CONFIG = {
    "dbname": "python_db",
    "user": "dev_user",
    "password": "12345"
}


def create_table():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS users(
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) NOT NULL,
                preferences JSONB,
                favorite_products INT[]
                );
                ''')
                conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при создании таблицы: {e}")


def insert_users():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            users_data = [
                ("user1", {"theme": "dark", "language": "ru"}, [2, 5, 8]),
                ("user2", {"theme": "light", "language": "en"}, [1, 4]),
                ("user3", {"theme": "dark", "language": "ru"}, [3, 6, 9]),
                ("user4", {"theme": "light", "language": "de"}, [2, 7]),
                ("user5", {"theme": "dark", "language": "en"}, [5, 10]),
            ]
            cursor.executemany('''
            INSERT INTO users (username, preferences, favorite_products) VALUES (%s, %s, %s);
            ''', [(u, json.dumps(p), f) for u, p, f in users_data])
            print("Данные успешно добавлены.")

        conn.commit()  # ✅ Только если всё прошло успешно

    except psycopg2.Error as e:
        if conn:
            conn.rollback()  # ❌ Откатываем транзакцию при ошибке
        print(f"Ошибка при добавлении данных: {e}")

    finally:
        if conn:
            conn.close()  # ✅ Закрываем соединение в любом случае


def search_users():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                SELECT * FROM users WHERE preferences->>'language' = 'ru'
                ''')
                result = cursor.fetchall()
                if result:
                    for row in result:
                        print(row)
                else:
                    print("Пользователи с языком 'ru' не найдены.")
    except psycopg2.Error as e:
        print(f"Ошибка при поиске пользователя: {e}")

def fav_users():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                SELECT id, username, preferences, favorite_products
                FROM users 
                WHERE 3 = ANY(favorite_products);
                ''')
                result = cursor.fetchall()
                if result:
                    print("🔹 Пользователи с любимым продуктом 3:")
                    for row in result:
                        user = {
                            "id": row[0],
                            "username": row[1],
                            "preferences": row[2],
                            "favorite_products": row[3]
                        }
                        print(json.dumps(user, indent=4, ensure_ascii=False))
                else:
                    print("❌ Пользователи с id(favorite_products) = 3 не найдены.")
    except psycopg2.Error as e:
        print(f"Ошибка при поиске пользователя: {e}")

def add_gin_index():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_users_preferences ON users USING GIN (preferences);
                CREATE INDEX IF NOT EXISTS idx_users_favorite_products ON users USING GIN (favorite_products);
                ''')
                conn.commit()
                print("✅ GIN-индексы успешно добавлены.")
    except psycopg2.Error as e:
        print(f"Ошибка при добавлении GIN-индексов: {e}")

def analyze_speed():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                print("🔹 Анализ скорости поиска по языку (JSONB)")
                cursor.execute("EXPLAIN ANALYZE SELECT * FROM users WHERE preferences->>'language' = 'ru';")
                print("\n".join(row[0] for row in cursor.fetchall()))

                print("\n🔹 Анализ скорости поиска по массиву (favorite_products)")
                cursor.execute("EXPLAIN ANALYZE SELECT * FROM users WHERE 3 = ANY(favorite_products);")
                print("\n".join(row[0] for row in cursor.fetchall()))

    except psycopg2.Error as e:
        print(f"Ошибка при анализе скорости запросов: {e}")
