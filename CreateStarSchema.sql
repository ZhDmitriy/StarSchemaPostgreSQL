CREATE TABLE customerDim (
    customer_id SERIAL PRIMARY KEY,
    age INTEGER,
    gender VARCHAR(50), 
    previous_purchases INTEGER, 
    frequency_purchases VARCHAR(100), 
    valid_dttm_from timestamp, 
    valid_dttm_to timestamp
)

CREATE TABLE shopDim (
    shop_id SERIAL PRIMARY KEY,
    location INTEGER,
    shipping_type VARCHAR(50), 
   	valid_dttm_from timestamp, 
    valid_dttm_to timestamp
)

CREATE TABLE productDim (
    product_id SERIAL PRIMARY KEY,
    item_purchased VARCHAR(150),
    category VARCHAR(100),
    size VARCHAR(50), 
    color VARCHAR(150), 
    season VARCHAR(150), 
    review_rating VARCHAR(50), 
   	valid_dttm_from timestamp, 
    valid_dttm_to timestamp
)

CREATE TABLE transactionDim (
    transaction_id SERIAL PRIMARY KEY,
    subscription_status VARCHAR(150),
    discount_applied VARCHAR(100),
    promo_code_used VARCHAR(50), 
    payment_method VARCHAR(150), 
   	valid_dttm_from timestamp, 
    valid_dttm_to timestamp
)

CREATE table dateTransactionDim (
    date_id SERIAL PRIMARY KEY,
    day timestamp,
   	month VARCHAR(50),
    year VARCHAR(50), 
    quarter VARCHAR(150)
)

CREATE table transactionFact (
    transaction_fact_Id SERIAL PRIMARY KEY,
   	fk_customer_id INT REFERENCES customerDim(customer_id),
   	fk_product_id INT REFERENCES productDim(product_id),
   	fk_transaction_id INT REFERENCES transactionDim(transaction_id),
   	fk_shop_id INT REFERENCES shopDim(shop_id),
   	fk_date_id INT REFERENCES dateTransactionDim(date_id),
   	purchase_amount FLOAT
)






