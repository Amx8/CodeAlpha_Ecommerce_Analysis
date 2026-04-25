CREATE DataBase EcommerceDB;
go
USE EcommerceDB;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name NVARCHAR(100),
	last_name NVARCHAR(100),
	email NVARCHAR(100),
    city NVARCHAR(100),
	country NVARCHAR(100),
	gender NVARCHAR(100),
	age INT,
	registration_date Date
);


CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    category_name NVARCHAR(100)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    title NVARCHAR(MAX),
    price DECIMAL(10,2),
    star_rating INT,
    in_stock bit,
	upc NVARCHAR(150),
	category_id INT,
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
	description NVARCHAR(MAX),
	availability_count INT,
	image_url NVARCHAR(MAX)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
	total_amount DECIMAL(10,2),
	status NVARCHAR(100),
	payment_method NVARCHAR(100),
	shipping_city NVARCHAR(100),
	shipping_country NVARCHAR(100),
);

CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
	unit_price DECIMAL(10,2),
	line_total DECIMAL(10,2),
);
CREATE TABLE reviews (
    review_id INT PRIMARY KEY,
    product_id INT,
	customer_id INT,
    review_text NVARCHAR(MAX),
	star_rating INT,
	review_date DATE,
	helpful_votes INT,
	verified_purchase BIT
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


INSERT INTO customers (customer_id, first_name,last_name ,email,city,country,gender,age,registration_date)
SELECT customer_id, first_name, last_name,email,city,country,gender,age,registration_date
FROM customers_temp;


INSERT INTO categories (category_id, category_name)
SELECT category_id, category_name
FROM categories_temp;

INSERT INTO products (product_id, title, price, star_rating, in_stock, upc, category_id, description, availability_count, image_url)
SELECT product_id, title, price, star_rating, in_stock, upc, category_id, description, availability_count, image_url
FROM products_temp;

INSERT INTO orders (order_id, customer_id, order_date, total_amount, status, payment_method, shipping_city, shipping_country)
SELECT order_id, customer_id, order_date, total_amount, status, payment_method, shipping_city, shipping_country
FROM orders_temp;

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, line_total)
SELECT order_item_id, order_id, product_id, quantity, unit_price, line_total
FROM order_items_temp;

INSERT INTO reviews (review_id, product_id, customer_id, review_text, star_rating, review_date, helpful_votes, verified_purchase)
SELECT review_id, product_id, customer_id, review_text, star_rating, review_date, helpful_votes, verified_purchase
FROM reviews_temp;

DROP TABLE IF EXISTS customers_temp;
DROP TABLE IF EXISTS categories_temp;
DROP TABLE IF EXISTS products_temp;
DROP TABLE IF EXISTS orders_temp;
DROP TABLE IF EXISTS order_items_temp;
DROP TABLE IF EXISTS reviews_temp;