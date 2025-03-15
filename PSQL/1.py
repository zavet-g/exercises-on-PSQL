import psycopg2

conn = psycopg2.connect(
    dbname="python_db",
    user="dev_user",
    password="12345",
    host="localhost"
)

cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author VARCHAR(100) NOT NULL,
    price INTEGER NOT NULL
    );
''')

conn.commit()

books_data = [
    ("1984", "Джордж Оруэлл", 500),
    ("Мастер и Маргарита", "Михаил Булгаков", 700),
    ("Преступление и наказание", "Фёдор Достоевский", 650),
]

try:
    cursor.executemany('''
    INSERT INTO books (title, author, price) VALUES (%s, %s, %s)
    ''', books_data)
    conn.commit()
except Exception as e:
    conn.rollback()

try:
    cursor.execute('''
    SELECT * FROM books WHERE price < 600
    ''')
    books = cursor.fetchall()
    print(books)
except Exception as e:
    conn.rollback()

try:
    cursor.execute('''
    UPDATE books SET price = 30 WHERE id = 1
    ''')
except Exception as e:
    conn.rollback()

try:
    cursor.execute('''
    DELETE FROM books WHERE id = '3'
    ''')
    conn.commit()
except Exception as e:
    conn.rollback()

cursor.close()
conn.close()
