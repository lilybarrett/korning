-- DEFINE YOUR DATABASE SCHEMA HERE
-- SCHEMA
	DROP TABLE IF EXISTS customers CASCADE;
	DROP TABLE IF EXISTS employees CASCADE;
	DROP TABLE IF EXISTS products;
	DROP TABLE IF EXISTS frequencies CASCADE;
	DROP TABLE IF EXISTS invoices;
	
	CREATE TABLE customers (
	  id SERIAL PRIMARY KEY,
	  name VARCHAR(100),
	  account_no VARCHAR(100)
	);


	CREATE TABLE employees (
	  id SERIAL PRIMARY KEY,
	  name VARCHAR(100),
	  email VARCHAR(100)
	);

	CREATE TABLE products (
	  id SERIAL PRIMARY KEY,
	  name VARCHAR(100)
	);

	CREATE TABLE frequencies(
	  id SERIAL PRIMARY KEY,
	  name VARCHAR(100)
	);

	CREATE TABLE invoices (
	  id SERIAL PRIMARY KEY,
	  employee_id INTEGER REFERENCES employees (id),
	  customer_id INTEGER REFERENCES customers (id),
	  frequency_id INTEGER REFERENCES frequencies (id),
	  invoice_no INTEGER,
	  sale_date DATE,
	  sale_amount NUMERIC,
	  units_sold INTEGER
	);
