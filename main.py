import mysql.connector
import requests
import random
from confluent_kafka import Producer

# Constants
BASE_URL = "https://randomuser.me/api/?nat=in&results=3"
PARTIES = ["BJP", "Congress", "Trinamool"]
random.seed(21)

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Change this to your Kafka broker address
}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

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

            candidate = {
                "candidate_id": str(idx + 1),
                "candidate_name": candidate_name,
                "party_affiliation": party_affiliation,
                "biography": biography,
                "campaign_platform": campaign_platform,
                "photo_url": photo_url
            }

            # Produce candidate to Kafka topic
            producer.produce('candidates_topic', key=candidate["candidate_id"], value=str(candidate), callback=delivery_report)

            candidates.append((
                candidate["candidate_id"],
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

def generate_voter_data(conn):
    try:
        response = requests.get(BASE_URL)
        if response.status_code != 200:
            print(f"Error fetching data from API: {response.status_code}")
            return
        
        data = response.json()
        voters = []

        for idx, person in enumerate(data['results']):
            voter = {
                "voter_id": str(random.randint(100000, 999999)),
                "voter_name": f"{person['name']['first']} {person['name']['last']}",
                "date_of_birth": person['dob']['date'],
                "gender": person['gender'],
                "nationality": person['nat'],
                "registration_number": f"REG{random.randint(1000, 9999)}",
                "address_street": person['location']['street']['name'],
                "address_city": person['location']['city'],
                "address_state": person['location']['state'],
                "address_country": person['location']['country'],
                "address_postcode": person['location']['postcode'],
                "email": person['email'],
                "phone_number": person['phone'],
                "cell_number": person['cell'],
                "picture": person['picture']['large'],
                "registered_age": person['dob']['age']
            }

            # Produce voter to Kafka topic
            producer.produce('voters_topic', key=voter["voter_id"], value=str(voter), callback=delivery_report)

            voters.append((
                voter["voter_id"],
                voter["voter_name"],
                voter["date_of_birth"],
                voter["gender"],
                voter["nationality"],
                voter["registration_number"],
                voter["address_street"],
                voter["address_city"],
                voter["address_state"],
                voter["address_country"],
                voter["address_postcode"],
                voter["email"],
                voter["phone_number"],
                voter["cell_number"],
                voter["picture"],
                voter["registered_age"]
            ))

        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO voters (
                voter_id, voter_name, date_of_birth, gender, nationality, registration_number,
                address_street, address_city, address_state, address_country, address_postcode,
                email, phone_number, cell_number, picture, registered_age
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, voters)
        conn.commit()
        print("New voters registered")
    except Exception as e:
        print(f"Error generating voter data: {e}")

def fetch_all_voters(conn):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM voters")
        voters = cursor.fetchall()
        return voters
    except Exception as e:
        print(f"Error fetching voters: {e}")
        return []

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
        
        voters = fetch_all_voters(mysql_conn)
        if not voters:
            print("No voters found, generating new voter data...")
            generate_voter_data(mysql_conn)
        else:
            print("Voters found:")
            for voter in voters:
                print(voter)
        
        mysql_conn.close()

    # Wait for any outstanding messages to be delivered and delivery reports received
    producer.flush()
