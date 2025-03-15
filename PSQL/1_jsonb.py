import psycopg2
import json

data = {
    "name": "Smartphone",
    "price": 599,
    "tags": ["electronics", "android"],
    "specs": {"ram": "8GB", "storage": "128GB"}
}

conn = psycopg2.connect(dbname="python_db", user="dev_user", password="secure_password")
cursor = conn.cursor()

cursor.execute("""
    INSERT INTO products (name, meta)
    VALUES (%s, %s);
""", (data["name"], json.dumps(data)))

conn.commit()
conn.close()

# -- Выбрать товары с ценой > 500
# SELECT * FROM products WHERE (meta->>'price')::int > 500;
#
# -- Выбрать товары с тегом "android"
# SELECT * FROM products WHERE meta->'tags' ? 'android';
#
# -- Обновить поле "price" в JSONB
# UPDATE products
# SET meta = jsonb_set(meta, '{price}', '699')
# WHERE id = 1;

# -- Индекс для поиска по ключам и значениям в JSONB
# CREATE INDEX idx_products_meta ON products USING GIN (meta);
#
# -- До создания индекса
# EXPLAIN ANALYZE SELECT * FROM products WHERE meta @> '{"tags": ["android"]}';
#
# -- После создания индекса
# EXPLAIN ANALYZE SELECT * FROM products WHERE meta @> '{"tags": ["android"]}';

# -- Добавить колонку для поискового вектора
# ALTER TABLE products ADD COLUMN search_vector TSVECTOR;
#
# -- Заполнить вектор данными из name и meta
# UPDATE products
# SET search_vector =
#     to_tsvector('english', name) ||
#     to_tsvector('english', meta->>'specs');

# CREATE INDEX idx_products_search ON products USING GIN (search_vector);