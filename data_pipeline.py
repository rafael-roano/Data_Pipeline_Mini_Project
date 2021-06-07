import mysql.connector
import pandas as pd

def get_db_connection():
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
    # Get the most popular ticket in the past month
    sql_statement ="SELECT event_name, COUNT(event_name) AS 'tickets_sold' FROM ticket_sales GROUP BY event_name ORDER BY tickets_sold DESC LIMIT 3"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records

def show_bullet_results(message, query_results, units):
    # Show query results in bullets format
    
    print(f"{message}")
    for bullet in query_results:
        print(f"    - {bullet[0]} ({units}: {bullet[1]})")

    return

connection = get_db_connection()
file_path_csv = "/home/jv/Python/SB/SB_Projects/Pipeline_Mini-Project/third_party_sales_1.csv"


# load_third_party(connection, file_path_csv)
popular = query_popular_tickets(connection)
show_bullet_results("Top-3 Most Popular Tickets in Past Month:", popular, "tickets sold")

# print(popular)
# print(type(popular))
# print(type(popular[1]))

# df = pd.read_csv(file_path_csv, header=None)
# print(type(int(df[0][0])))
# print(type(df[1][0]))
# print(type(int(df[2][0])))
# print(type(df[3][0]))
# print(type(df[4][0]))
# print(type(df[5][0]))
# print(type(df[6][0]))
# print(type(int(df[7][0])))
# print(type(float(df[8][0])))
# print(type(int(df[9][0])))