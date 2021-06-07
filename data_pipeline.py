import mysql.connector
import pandas as pd

def get_db_connection():
    # Set up connection with MySQL database

    connection =None

    try:
        connection = mysql.connector.connect(user='root',
                            password='mArtisan3!',
                            host='localhost',
                            port='3306',
                            database='data_pipeline')
    
    except Exception as error:
        print("Error while connecting to database",error)
    
    return connection


def load_third_party(connection, file_path_csv):
    # Load csv file to MySQL Table
    
    df = pd.read_csv(file_path_csv, header=None)
    cursor = connection.cursor()
    query = "INSERT INTO ticket_sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # [Iterate through the CSV file and execute insertstatement]

    for index, row in df.iterrows():

        val = (int(row[0]), row[1], int(row[2]), row[3], row[4], row[5], row[6], int(row[7]), float(row[8]), int(row[9]))
        cursor.execute(query, val)
        connection.commit()

    cursor.close()
    return

def query_popular_tickets(connection):
    # Get the Top-3 most popular event tickets in the past month
    sql_statement ="SELECT event_name, SUM(num_tickets) AS 'tickets_sold' FROM ticket_sales GROUP BY event_name ORDER BY tickets_sold DESC LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def query_expensive_tickets(connection):
    # Get the Top-3 most expensive event tickets in the past month
    sql_statement ="SELECT event_name, ROUND(AVG(price),2) AS 'price' FROM ticket_sales GROUP BY event_name ORDER BY price DESC LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def query_popular_event(connection):
    # Get the Top-3 most popular events in the past month
    sql_statement ="SELECT event_type, SUM(num_tickets) AS 'tickets_sold' FROM ticket_sales GROUP BY event_type ORDER BY tickets_sold DESC LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def query_cities_event(connection):
    # Get the Top-3 cities with more events in the past month
    sql_statement ="SELECT event_city, COUNT(event_city) AS 'event_n' FROM ticket_sales GROUP BY event_city ORDER BY event_n DESC LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def show_bullet_results(message, query_results, units):
    # Show query results in bullets format
    
    print(f"{message}")
    for bullet in query_results:
        print(f"    - {bullet[0]} ({units}{bullet[1]})")

    return

connection = get_db_connection()
file_path_csv = "/home/jv/Python/SB/SB_Projects/Pipeline_Mini-Project/third_party_sales_1.csv"

load_third_party(connection, file_path_csv)

popular = query_popular_tickets(connection)
show_bullet_results("Top-3 Most Popular Events in Past Month:", popular, "tickets sold: ")
print()
expensive = query_expensive_tickets(connection)
show_bullet_results("Top-3 Most Expensive Events in Past Month:", expensive, "average price: $")
print()
event_type = query_popular_event(connection)
show_bullet_results("Top-3 Most Popular Event Types in Past Month:", event_type, "tickets sold: ")
print()
cities = query_cities_event(connection)
show_bullet_results("Top-3 Cities with More Events in Past Month:", cities, "events: ")