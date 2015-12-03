# Use this file to import the sales information into the
# the database.
require "pg"
require 'csv'
require 'pry'

  system 'psql korning < schema.sql'

  def db_connection
    begin
      connection = PG.connect(dbname: "korning")
      yield(connection)
    ensure
      connection.close
    end
  end

  def get_id(name, table)
    query = "SELECT t.id FROM #{table} t WHERE t.name = ($1)"
    response = db_connection do |conn|
      conn.exec_params(query, [name])
    end
    response.first['id'].to_i
  end

  def customers
  @info.map { |row|
    { customer_name: row[:customer_name], account_no: row[:account_no] }
    }.uniq
  end

  def products
    @info.map { |row| { product_name: row[:product_name] } }.uniq
  end

  def frequencies
    @info.map { |row| { invoice_frequency: row[:invoice_frequency] } }.uniq
  end

  def employees
    @info
    .map { |row| { employee_name: row[:employee_name], email: row[:email] } }
    .uniq
  end

  @info = CSV
    .readlines('sales.csv', headers: true, header_converters: :symbol)
    .map(&:to_hash)
    .map do |row|
    row.map { |key, value|
      if key == :employee
        name = row[:employee].split[0..1].join(' ')
        email = row[:employee].split[2][1..-2]
        { employee_name: name, email:  email }
      elsif key == :customer_and_account_no
        name = row[:customer_and_account_no].split[0]
        account_no = row[:customer_and_account_no].split[1][1..-2]
        { customer_name: name, account_no: account_no }
      else
        { key => value }
      end
    }.reduce(:merge)
  end

  db_connection do |conn|
    employees.each do |row|
    query = 'INSERT INTO employees (name, email) VALUES ($1, $2)'
    conn.exec_params(query, [row[:employee_name], row[:email]])
  end

  customers.each do |row|
    query = 'INSERT INTO customers (name, account_no) VALUES ($1, $2)'
    conn.exec_params(query, [row[:customer_name], row[:account_no]])
  end

  products.each do |row|
    query = 'INSERT INTO products (name) VALUES ($1)'
    conn.exec_params(query, [row[:product_name]])
  end

  frequencies.each do |row|
    query = 'INSERT INTO frequencies (name) VALUES ($1)'
    conn.exec_params(query, [row[:invoice_frequency]])
  end

  @info.each do |row|
    employee_id = get_id(row[:employee_name], 'employees')
    customer_id = get_id(row[:customer_name], 'customers')
    frequency_id = get_id(row[:invoice_frequency], 'frequencies')
    invoice_no = row[:invoice_no].to_i
    sale_date = row[:sale_date]
    sale_amount = row[:sale_amount][1..-1].to_f
    units_sold = row[:units_sold].to_i
    conn.exec_params(
     'INSERT INTO invoices
       (employee_id, customer_id, frequency_id, invoice_no,
        sale_date, sale_amount, units_sold)
     VALUES ($1, $2, $3, $4, $5, $6, $7)',
     [employee_id, customer_id, frequency_id, invoice_no,
       sale_date, sale_amount, units_sold]
  )
  end
end
