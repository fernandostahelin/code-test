CREATE TABLE IF NOT EXISTS fact_orders (
    order_detail_id int PRIMARY KEY,
    order_id int,
    product_key int,
    customer_key int,
    date_key int,
    geography_key int,
    quantity int,
    unit_price decimal,
    discount decimal,
    sales_amount decimal,
    order_status varchar
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key int PRIMARY KEY,
    full_date date,
    year int,
    quarter int,
    month int,
    month_name varchar,
    week int,
    day_of_week int,
    day_name varchar,
    fiscal_year int,
    fiscal_quarter int
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key int PRIMARY KEY,
    customer_id varchar,
    company_name varchar,
    contact_name varchar,
    contact_title varchar,
    customer_type_id varchar,
    phone varchar,
    email varchar
);

CREATE TABLE IF NOT EXISTS dim_customer_type (
    customer_type_id varchar PRIMARY KEY,
    customer_type_description varchar
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_key int PRIMARY KEY,
    product_id int,
    product_name varchar,
    category_id int,
    category_name varchar,
    supplier_id int,
    supplier_name varchar,
    unit_price decimal,
    discontinued boolean
);

CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key int PRIMARY KEY,
    city varchar,
    region varchar,
    country varchar,
    postal_code varchar
);

ALTER TABLE fact_orders ADD FOREIGN KEY (
    product_key
) REFERENCES dim_products (product_key);

ALTER TABLE fact_orders ADD FOREIGN KEY (
    customer_key
) REFERENCES dim_customers (customer_key);

ALTER TABLE fact_orders ADD FOREIGN KEY (date_key) REFERENCES dim_date (
    date_key
);

ALTER TABLE fact_orders ADD FOREIGN KEY (
    geography_key
) REFERENCES dim_geography (geography_key);

ALTER TABLE dim_customers ADD FOREIGN KEY (
    customer_type_id
) REFERENCES dim_customer_type (customer_type_id);
