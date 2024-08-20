import mysql.connector

def connect_mysql():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            port="3306",  # Default MySQL port
        )
        print("Connected to MySQL server")
        return conn
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_database(conn, db_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")  # Select the database
        print(f"Database '{db_name}' is ready")
    except Exception as e:
        print(f"Error creating database: {e}")

def create_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id VARCHAR(255) PRIMARY KEY,
                candidate_name VARCHAR(255),
                party_affiliation VARCHAR(255),
                biography TEXT,
                campaign_platform TEXT,
                photo_url TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS voters (
                voter_id VARCHAR(255) PRIMARY KEY,
                voter_name VARCHAR(255),
                date_of_birth VARCHAR(255),
                gender VARCHAR(255),
                nationality VARCHAR(255),
                registration_number VARCHAR(255),
                address_street VARCHAR(255),
                address_city VARCHAR(255),
                address_state VARCHAR(255),
                address_country VARCHAR(255),
                address_postcode VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(255),
                cell_number VARCHAR(255),
                picture TEXT,
                registered_age INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                voter_id VARCHAR(255) UNIQUE,
                candidate_id VARCHAR(255),
                voting_time TIMESTAMP,
                vote int DEFAULT 1,
                PRIMARY KEY (voter_id, candidate_id)
            )
        """)

        conn.commit()
        print("Tables are ready")
    except Exception as e:
        print(f"Error creating tables: {e}")

if __name__ == "__main__":
    # Connect to MySQL (XAMPP)
    mysql_conn = connect_mysql()

    if mysql_conn:
        # Check and create the database if it doesn't exist
        create_database(mysql_conn, "vote")
        create_tables(mysql_conn)

        # Close the connection
        mysql_conn.close()
