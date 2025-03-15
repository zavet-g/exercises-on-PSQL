import psycopg2

DB_CONFIG = {
    "dbname": "python_db",
    "user": "dev_user",
    "password": "12345"
}


def create_table_in_db():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                       CREATE TABLE IF NOT EXISTS orders(
                           id SERIAL PRIMARY KEY,
                           name VARCHAR(100),
                           product_ids INT[]
                       );
                   ''')
                conn.commit()
    except psycopg2.Error as e:
        print(f"Ошибка при создании таблицы: {e}")
        conn.rollback()

def add_products_in_array():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO orders (name, product_ids)
                    VALUES (%s, %s);
                ''', ("Орешник", [1, 5, 7]))

    except psycopg2.Error as e:
        print(f"Ошибка при добавлении заказов в таблицу: {e}")
        conn.rollback()

def search_array():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                SELECT * FROM orders WHERE 5 = ANY(product_ids);
                ''')

                result = cursor.fetchall()
                for row in result:
                    print(row)

    except psycopg2.Error as e:
        print(f"Ошибка при поиске заказов в таблице: {e}")
        conn.rollback()

create_table_in_db()
add_products_in_array()
search_array()