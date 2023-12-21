import psycopg2

db_params = {
    'dbname': 'postgre',
    'user': 'root',
    'password': 'Pa$$w0rd',
    'host': 'localhost',
    'port': '5431'
}

try:
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            query1 = """
                    SELECT c.*, SUM(o.total_amount) AS total_order_amount
                    FROM customers c
                    JOIN orders o ON c.customer_id = o.customer_id
                    WHERE EXTRACT(MONTH FROM o.order_date) = EXTRACT(MONTH FROM CURRENT_DATE)
                    GROUP BY c.customer_id
                    ORDER BY total_order_amount DESC;
                """

            query2 = """
                    SELECT p.*
                    FROM products p
                    JOIN (
                      SELECT category, AVG(price) AS avg_price
                      FROM products
                      GROUP BY category
                    ) avg_prices ON p.category = avg_prices.category
                    WHERE p.price > avg_prices.avg_price
                    ORDER BY p.price ASC;
                """

            cursor.execute(query1)
            result1 = cursor.fetchall()

            cursor.execute(query2)
            result2 = cursor.fetchall()

    print("Результат запроса 1:", result1)
    print("Результат запроса 2:", result2)

except psycopg2.Error as e:
    print("Ошибка при работе с базой данных:", e)
