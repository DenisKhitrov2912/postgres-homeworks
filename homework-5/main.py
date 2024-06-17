import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE {db_name}")
    except Exception:
        cur.execute(f"DROP DATABASE {db_name}")
        cur.execute(f"CREATE DATABASE {db_name}")
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as file:
        create_db_script = file.read()
    cur.execute(create_db_script)
    cur.execute(
        """
        ALTER TABLE products ADD COLUMN supplier_id integer
        """
    )


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""CREATE TABLE suppliers 
    (supplier_id serial primary key,
    company_name varchar,
    contact varchar,
    address varchar,
    phone varchar(20),
    fax varchar(20),
    homepage varchar,
    products text)""")


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r') as file:
        data = json.load(file)
        return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for data in suppliers:
        cur.execute("""
        INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING supplier_id""",
                    (data["company_name"], data["contact"], data["address"], data["phone"], data["fax"],
                     data["homepage"], data["products"]))

        supplier_id = cur.fetchone()[0]

        for product_one in data['products']:
            cur.execute(
                """ SELECT product_id FROM products WHERE product_name = %(product)s""", {'product': product_one}
            )

            product_id = cur.fetchone()[0]

            cur.execute(
                """UPDATE products SET supplier_id = %(suppl)s WHERE product_id= %(prod)s""",
                {'suppl': supplier_id, 'prod': product_id}
            )


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""

    cur.execute(
        """ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers; """
    )


if __name__ == '__main__':
    main()
