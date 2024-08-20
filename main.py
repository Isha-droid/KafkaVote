import mysql.connector
import requests
import random

BASE_URL = "https://randomuser.me/api/?nat=in&results=3"
PARTIES = ["BJP", "Congress", "Trinamool"]
random.seed(21)

def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="sample",
            password="password",
            port="3306",  
            database="test"
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

def fetch_all_candidates(conn):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM candidates")
        candidates = cursor.fetchall()
        return candidates
    except Exception as e:
        print(f"Error fetching candidates: {e}")
        return []

def register_new_candidates(conn):
    try:
        response = requests.get(BASE_URL)
        if response.status_code != 200:
            print(f"Error fetching data from API: {response.status_code}")
            return
        
        data = response.json()
        candidates = []

        for idx, person in enumerate(data['results']):
            gender = 'male' if idx % 2 == 0 else 'female'
            party_affiliation = random.choice(PARTIES)
            candidate_name = f"{person['name']['first']} {person['name']['last']}"
            biography = f"{candidate_name} is a candidate from the {party_affiliation} party."
            campaign_platform = f"Focuses on development, education, and healthcare."
            photo_url = person['picture']['large']

            candidates.append((
                str(idx + 1),
                candidate_name,
                party_affiliation,
                biography,
                campaign_platform,
                photo_url
            ))

        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, candidates)
        conn.commit()
        print("New candidates registered")
    except Exception as e:
        print(f"Error registering new candidates: {e}")

if __name__ == "__main__":
    mysql_conn = connect_mysql()

    if mysql_conn:
        create_database(mysql_conn, "vote")
        create_tables(mysql_conn)

        candidates = fetch_all_candidates(mysql_conn)
        if not candidates:
            print("No candidates found, registering new candidates...")
            register_new_candidates(mysql_conn)
        else:
            print("Candidates found:")
            for candidate in candidates:
                print(candidate)
        
        mysql_conn.close()
