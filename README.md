# Data Pipeline Mini Project

This is a basic data pipeline to assist a Ticket System designed for online ticket selling and purchasing. Tickets are for events like concerts and sports matches. This system allows individuals to buy tickets directly through the platform and also manage ticket sales for third party resellers.

The pipeline is developed in Python, supports the third-party operations and main functions are:

* Ingestion of sales data from CSV file
* Transformaton/validation of data types
* Loading of validated data into MySQL table
* Displaying of statistical information

This is a Data Pipeline Mini Project, part of Springboard’s Data Engineering Career Track program. 

## Getting Started

### Prerequisites

* mysql.connector (pip install mysql-connector-python)
* pandas (pip install pandas)

### Installing

Repository can be clone from https://github.com/rafael-roano/Data_Pipeline_Mini_Project.git. This includes:

* DDL to create ticket_sales table: ticket_sales_table_DDL.sql
* CSV file with sales data to populate MySQL table: third_party_sales_1.csv
* Pipeline module: data_pipeline.py
* Results (statitiscal information): command_line_execution_log.txt

Prior to testing the module:

1. The ticket_sales table needs to be created by running ticket_sales_table_DDL.sql file
2. Edit database credentials in get_db_connection()
3. Edit url in file_path_csv varialble accordongly 

After running code, results should be as presented in file command_line_execution_log.txt

## Authors

* **Rafael Roano**
