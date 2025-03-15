import psycopg2

DB_CONFIG = {
    "dbname": "python_db",
    "user": "dev_user",
    "password": "12345"
}


def create_db():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            witch_type = input('\n Введите тип таблицы для создания:'
                               '\n USER1 для таблицы с INT[]'
                               '\n USER2 для таблицы c REAL').strip()
            if witch_type == 'USER1':
                try:
                    with conn.cursor() as cursor:
                        cursor.execute('''
                        CREATE TABLE IF NOT EXISTS users1(
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(200),
                        hobbies INT[]
                        );
                        ''')
                except psycopg2.Error as e:
                    print(f'Ошибка: {e}')
            else:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute('''
                        CREATE TABLE IF NOT EXISTS users2(
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(200),
                        hobbies REAL
                        );
                        ''')
                except psycopg2.Error as e:
                    print(f'Ошибка: {e}')

    except psycopg2.Error as e:
        print(f'Ошибка: {e}')


def add_user():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                users_data = [
                    ("user1", [2, 5, 8]),
                    ("user2", [1, 4]),
                ]
                cursor.executemany('''
                INSERT INTO users (name, hobbies)VALUES (%s, %s);
                ''', [(u, n) for u, n in users_data])

    except psycopg2.Error as e:
        print(f'Ошибка: {e}')

def insert_user():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                SELECT * FROM users WHERE name = 'user1'
                ''')
                name = cursor.fetchone()
                print(name)
    except psycopg2.Error as e:
        print(f'Ошибка: {e}')

create_db()
add_user()
insert_user()