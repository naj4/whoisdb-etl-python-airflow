import psycopg2
from db_config import get_db_info
import glob
import pandas as pd
import os
import numpy as np
from constants import DESTINATION_DIR

filename='db_info.ini'
section='postgres-whois-db'
config_path = os.path.join('config', filename) 
db_info = get_db_info(config_path,section)

# SQL queries
create_sourceconfig_table_query = """
CREATE TABLE IF NOT EXISTS SourceConfig(
    ConfigId SERIAL PRIMARY KEY,
    ConfigValue VARCHAR(500) UNIQUE,
    CreatedBy VARCHAR(200) DEFAULT current_user,
    CreatedOn TIMESTAMP DEFAULT current_timestamp
);
"""
create_domainwhoisinfo_table_query = """
CREATE TABLE IF NOT EXISTS domainwhoisinfo (
    domainName VARCHAR(500),
    registrarName VARCHAR(500),
    contactEmail VARCHAR(500),
    whoisServer VARCHAR(500),
    nameServers VARCHAR(500),
    createdDate DATE,
    updatedDate DATE,
    expiresDate DATE,
    standardRegCreatedDate DATE,
    standardRegUpdatedDate DATE,
    standardRegExpiresDate DATE,
    status VARCHAR(500),
    Audit_auditUpdatedDate DATE,
    registrant_email VARCHAR(500),
    registrant_name VARCHAR(500),
    registrant_organization VARCHAR(500),
    registrant_street1 VARCHAR(500),
    registrant_street2 VARCHAR(500),
    registrant_street3 VARCHAR(500),
    registrant_street4 VARCHAR(500),
    registrant_city VARCHAR(500),
    registrant_state VARCHAR(500),
    registrant_postalCode VARCHAR(500),
    registrant_country VARCHAR(500),
    registrant_fax VARCHAR(50),
    registrant_faxExt VARCHAR(50),
    registrant_telephone VARCHAR(50),
    registrant_telephoneExt VARCHAR(50),
    administrativeContact_email VARCHAR(500),
    administrativeContact_name VARCHAR(500),
    administrativeContact_organization VARCHAR(500),
    administrativeContact_street1 VARCHAR(500),
    administrativeContact_street2 VARCHAR(500),
    administrativeContact_street3 VARCHAR(500),
    administrativeContact_street4 VARCHAR(500),
    administrativeContact_city VARCHAR(500),
    administrativeContact_state VARCHAR(500),
    administrativeContact_postalCode VARCHAR(500),
    administrativeContact_country VARCHAR(500),
    administrativeContact_fax VARCHAR(50),
    administrativeContact_faxExt VARCHAR(50),
    administrativeContact_telephone VARCHAR(50),
    administrativeContact_telephoneExt VARCHAR(50)
);
"""

insert_data_query = """
INSERT INTO SourceConfig (ConfigValue)
    SELECT %s WHERE NOT EXISTS (
        SELECT 1 FROM SourceConfig WHERE ConfigValue = %s
    );
    """

select_data_query = """
SELECT ConfigValue FROM SourceConfig;
"""

file_names = [
    'sample_com_v47_simple_1000.csv.gz',
    'sample_net_v47_simple_1000.csv.gz',
    'sample_org_v47_simple_1000.csv.gz',
    'sample_dev_v47_simple_1000.csv.gz',
    'sample_info_v47_simple_1000.csv.gz'
]

# Function to execute a query
def execute_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        connection.commit()

# Function to fetch data from the database
def fetch_data(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()
    
def create_table(connection, query, table_name):
    execute_query(connection, query)
    print(f"Table {table_name} processed successfully.")

def insert_values(connection):
    with connection.cursor() as cursor:
        for name in file_names:
            cursor.execute(insert_data_query, (name, name))
            if cursor.rowcount == 0:
                print(f"The file '{name}' already exists in table.")

        connection.commit()        
    print("Filenames processed successfully.")

def get_file_names(connection):
    rows = fetch_data(connection, select_data_query)
    file_names = [row[0] for row in rows]
    for file_name in file_names:
        print(file_name)
    return file_names

def insert_data_from_csv(connection, directory):
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    for file in csv_files:
        df = pd.read_csv(file, parse_dates=['createdDate', 'updatedDate', 'expiresDate', 'standardRegCreatedDate', 'standardRegUpdatedDate', 'standardRegExpiresDate', 'Audit_auditUpdatedDate'])

        # Replace NaT with None and NaN with None for all columns
        df.replace({pd.NaT: None, np.nan: None}, inplace=True)
      
        # # Replace NaN with None for other columns
        # df = df.where(pd.notnull(df), None)

        # Generate insert query dynamically
        # insert_query = sql.SQL("INSERT INTO domainwhoisinfo ({}) VALUES ({})").format(
        #     sql.SQL(', ').join(map(sql.Identifier, df.columns)),
        #     sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
        # )

        # Directly join column names without quoting
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))

        # Construct the SQL query
        insert_query = f"INSERT INTO domainwhoisinfo ({columns}) VALUES ({placeholders})"        

        with connection.cursor() as cursor:
            for row in df.itertuples(index=False, name=None):
                try:
                    cursor.execute(insert_query, row)
                except Exception as e:
                    print(f"Error inserting row: {row}")
                    print(f"Error message: {e}")
                    # continue  # Skip this row and continue with the next one
                    break
            connection.commit() 

def db_setup():
    connection = None
    # Establish connection to the database
    try:
        connection = psycopg2.connect(**db_info)
        print("Connection to the database established successfully.")
    
        create_table(connection, create_sourceconfig_table_query, 'SourceConfig')         
        insert_values(connection)
        create_table(connection, create_domainwhoisinfo_table_query, 'DomainWhoIsInfo')  
        return get_file_names(connection)
        
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

def load_files_to_db():
    directory = DESTINATION_DIR
    connection = None
    # Establish connection to the database
    try:
        connection = psycopg2.connect(**db_info)
        print("Connection to the database established successfully.")
       
        create_table(connection, create_domainwhoisinfo_table_query, 'DomainWhoIsInfo')
        insert_data_from_csv(connection, directory)
        
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")