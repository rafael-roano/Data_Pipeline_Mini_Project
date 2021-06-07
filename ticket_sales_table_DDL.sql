CREATE TABLE IF NOT EXISTS ticket_sales (
    ticket_id INT,
    trans_date DATE,
    event_id INT,
    event_name VARCHAR(50),
    event_date DATE,
    event_type VARCHAR(50),
    event_city VARCHAR(20),
    customer_id INT,
	price DECIMAL(10,2),
    num_tickets INT
);