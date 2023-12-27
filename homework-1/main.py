"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os

list_customers = []
list_employees = []
list_orders = []
pass_postgresql = os.getenv('PASS_POSTGRESQL')
print(pass_postgresql)
with open(os.path.join('north_data', 'customers_data.csv'), 'r') as file_one:
    customers_data = csv.reader(file_one)
    with open(os.path.join('north_data', 'employees_data.csv'), 'r') as file_two:
        employees_data = csv.reader(file_two)
        with open(os.path.join('north_data', 'orders_data.csv'), 'r') as file_three:
            orders_data = csv.reader(file_three)
            conn = psycopg2.connect(host='localhost', database='north', user='postgres', password=os.getenv('PASS_POSTGRESQL'))
            for row in customers_data:
                list_customers.append(row)
            for row in employees_data:
                list_employees.append(row)
            for row in orders_data:
                list_orders.append(row)
            try:
                with conn:
                    with conn.cursor() as cur:
                        for one in list_customers:
                            if one[0] == "customer_id":
                                continue
                            cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (one[0], one[1], one[2]))
                        cur.execute("SELECT * FROM customers")
                        rows = cur.fetchall()
                        for row in rows:
                            print(row)
                    with conn.cursor() as cur:
                        for one in list_employees:
                            if one[0] == "employee_id":
                                continue
                            cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (one[0], one[1], one[2], one[3], one[4], one[5]))
                        cur.execute("SELECT * FROM employees")
                        rows = cur.fetchall()
                        for row in rows:
                            print(row)
                    with conn.cursor() as cur:
                        for one in list_orders:
                            if one[0] == "order_id":
                                continue
                            cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", (one[0], one[1], one[2], one[3], one[4]))
                        cur.execute("SELECT * FROM orders")
                        rows = cur.fetchall()
                        for row in rows:
                            print(row)
            finally:
                conn.close()
