""" ETL процесс по загрузке данных в схему Звезда """

import psycopg2
import pandas as pd
from typing import NoReturn
from datetime import datetime as dt

class StarETL:

    def __init__(self, HOST, DB_NAME, USER, PASSWORD):
        self.connection = psycopg2.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB_NAME
        )

    def extract_transform(self, path_to_file: str) -> pd.DataFrame:
        """ Преобразовываем данные для загрузки """
        data = pd.read_csv(path_to_file)
        return data

    def insert_dimension_table(self, data: pd.DataFrame) -> NoReturn:
        """ Вставка данных в таблицы 'Измерений' """

        # Customer Dimension
        customerDim = data[['Age', 'Gender', 'Previous Purchases', 'Frequency of Purchases']].drop_duplicates()
        customerDim.columns = ['Age', 'Gender', 'Previous_Purchases', 'Frequency_of_Purchases']
        customerDim.reset_index(drop=True, inplace=True)
        customerDim = customerDim.sort_values('Age')

        cursor = self.connection.cursor()

        for customer in customerDim.itertuples():

            cursor.execute("""
                INSERT INTO customerDim (customer_id, age, gender, previous_purchases, frequency_purchases, valid_dttm_from, valid_dttm_to)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    age = EXCLUDED.age,
                    gender = EXCLUDED.gender,
                    previous_purchases = EXCLUDED.previous_purchases,
                    frequency_purchases = EXCLUDED.frequency_purchases,
                    valid_dttm_from = EXCLUDED.valid_dttm_from,
                    valid_dttm_to = EXCLUDED.valid_dttm_to
                """, (
                    customer.Index,
                    customer.Age,
                    customer.Gender,
                    customer.Previous_Purchases,
                    customer.Frequency_of_Purchases,
                    dt.now(),
                    dt.max)
                )

        # Product Dimension
        productDim = data[['Item Purchased', 'Category', 'Size', 'Color', 'Season', 'Review Rating']].drop_duplicates()
        productDim.columns = ['Item_Purchased', 'Category', 'Size', 'Color', 'Season', 'Review_Rating']
        productDim.reset_index(drop=True, inplace=True)

        for product in productDim.itertuples():

            cursor.execute("""
                INSERT INTO productDim (product_id, item_purchased, category, size, color, season, review_rating, valid_dttm_from, valid_dttm_to)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    item_purchased = EXCLUDED.item_purchased,
                    category = EXCLUDED.category,
                    size = EXCLUDED.size,
                    color = EXCLUDED.color,
                    season = EXCLUDED.season,
                    review_rating = EXCLUDED.review_rating,
                    valid_dttm_from = EXCLUDED.valid_dttm_from,
                    valid_dttm_to = EXCLUDED.valid_dttm_to
                """, (
                    product.Index,
                    product.Item_Purchased,
                    product.Category,
                    product.Size,
                    product.Color,
                    product.Season,
                    product.Review_Rating,
                    dt.now(),
                    dt.max)
                )

        # Transaction Dimension
        transactionDim = data[['Subscription Status', 'Discount Applied', 'Promo Code Used', 'Payment Method']].drop_duplicates()
        transactionDim.columns = ['Subscription_Status', 'Discount_Applied', 'Promo_Code_Used', 'Payment_Method']
        transactionDim.reset_index(drop=True, inplace=True)

        for transaction in transactionDim.itertuples():

            cursor.execute("""
                INSERT INTO transactionDim (transaction_id, subscription_status, discount_applied, promo_code_used, payment_method, valid_dttm_from, valid_dttm_to)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    subscription_status = EXCLUDED.subscription_status,
                    discount_applied = EXCLUDED.discount_applied,
                    promo_code_used = EXCLUDED.promo_code_used,
                    payment_method = EXCLUDED.payment_method,
                    valid_dttm_from = EXCLUDED.valid_dttm_from,
                    valid_dttm_to = EXCLUDED.valid_dttm_to
                """, (
                    transaction.Index,
                    transaction.Subscription_Status,
                    transaction.Discount_Applied,
                    transaction.Promo_Code_Used,
                    transaction.Payment_Method,
                    dt.now(),
                    dt.max)
                )

        # Shop Dimension
        shopDim = data[['Location', 'Shipping Type']].drop_duplicates()
        shopDim.columns = ['Location', 'Shipping_Type']
        shopDim.reset_index(drop=True, inplace=True)

        for shop in shopDim.itertuples():
            cursor.execute("""
                INSERT INTO shopDim (shop_id, location, shipping_type, valid_dttm_from, valid_dttm_to)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (shop_id) DO UPDATE SET
                    location = EXCLUDED.location,
                    shipping_type = EXCLUDED.shipping_type,
                    valid_dttm_from = EXCLUDED.valid_dttm_from,
                    valid_dttm_to = EXCLUDED.valid_dttm_to
                """, (
                shop.Index,
                shop.Location,
                shop.Shipping_Type,
                dt.now(),
                dt.max)
                           )

        self.connection.commit()
        cursor.close()

    def insert_fact_table(self, data: pd.DataFrame) -> NoReturn:
        """ Вставка данных в таблицу 'Фактов' """

        data.columns = ['index', 'Age', 'Gender', 'Item_Purchased', 'Category', 'Purchase_Amount_USD', 'Location', 'Size', 'Color', 'Season', 'Review_Rating', 'Subscription_Status',
                        'Shipping_Type', 'Discount_Applied', 'Promo_Code_Used', 'Previous_Purchases', 'Payment_Method', 'Frequency_of_Purchases']

        # Получение первичных ключей таблиц измерений

        fk_shop_id = 0

        cursor = self.connection.cursor()
        for dataRow in data.itertuples():

            cursor.execute(f"""
                SELECT customer_id
                FROM customerDim 
                WHERE age = {dataRow.Age} AND gender like '{dataRow.Gender}' AND previous_purchases = {dataRow.Previous_Purchases} 
                AND frequency_purchases like '{dataRow.Frequency_of_Purchases}'
            """)

            fk_customer_id = cursor.fetchall()[0][0]

            cursor.execute(f"""
                SELECT product_id
                FROM productDim 
                WHERE item_purchased = '{dataRow.Item_Purchased}' AND category like '{dataRow.Category}' AND size = '{dataRow.Size}' 
                AND color like '{dataRow.Color}' AND season = '{dataRow.Season}' AND review_rating = '{dataRow.Review_Rating}'
            """)

            fk_product_id = cursor.fetchall()[0][0]

            cursor.execute(f"""
                SELECT transaction_id
                FROM transactionDim 
                WHERE subscription_status = '{dataRow.Subscription_Status}' AND discount_applied like '{dataRow.Discount_Applied}' 
                AND promo_code_used = '{dataRow.Promo_Code_Used}' AND payment_method like '{dataRow.Payment_Method}'
            """)

            fk_transaction_id = cursor.fetchall()[0][0]

            cursor.execute(f"""
                SELECT shop_id
                FROM shopDim 
                WHERE location = '{dataRow.Location}' AND shipping_type like '{dataRow.Shipping_Type}'
            """)

            fk_shop_id = cursor.fetchall()[0][0]

            cursor.execute("""
                INSERT INTO transactionFact (transaction_fact_id, fk_customer_id, fk_product_id, fk_transaction_id, fk_shop_id, purchase_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_fact_id) DO NOTHING
                """, (
                    dataRow.Index,
                    fk_customer_id,
                    fk_product_id,
                    fk_transaction_id,
                    fk_shop_id,
                    dataRow.Purchase_Amount_USD
                ))
            self.connection.commit()
        cursor.close()



if __name__ == '__main__':
    starETL = StarETL(
        HOST='localhost',
        DB_NAME='FactTable',
        PASSWORD='postgres',
        USER='newuser'
    )


    data = starETL.extract_transform(
        path_to_file='/home/dmitriy/PycharmProjects/StarSchema/data/shopping_behavior_updated.csv'
    )
    #starETL.insert_dimension_table(data=data)
    starETL.insert_fact_table(data=data)