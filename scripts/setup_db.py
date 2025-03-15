import mysql.connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database credentials
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = "medical_data"

def connect_to_mysql():
    """Establish a connection to the MySQL server."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def create_database(conn):
    """Create the database if it does not exist."""
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME};")
    print(f"Database '{DB_NAME}' is ready.")
    cursor.close()

def connect_to_database():
    """Connect to the specified MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

def create_tables(conn):
    """Create all required tables in the database."""
    cursor = conn.cursor()

    TABLES = {
        "patient_demographics": """
            CREATE TABLE IF NOT EXISTS patient_demographics (
                patient_id VARCHAR(50) PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                DOB DATE,
                gender ENUM('Male', 'Female', 'Other', 'Unknown'),
                race VARCHAR(50),
                ethnicity VARCHAR(50),
                marital_status VARCHAR(50),
                street VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(50),
                postal_code VARCHAR(20),
                home_phone VARCHAR(20),
                mobile_phone VARCHAR(20),
                email VARCHAR(100),
                language VARCHAR(50)
            );
        """,
        "patient_hospitalizations": """
            CREATE TABLE IF NOT EXISTS patient_hospitalizations (
                hospitalization_id VARCHAR(50) PRIMARY KEY,
                patient_id VARCHAR(50),
                admission_date DATETIME,
                discharge_date DATETIME,
                hospital_name VARCHAR(255),
                service_details TEXT,
                FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE
            );
        """,
        "patient_diagnoses": """
            CREATE TABLE IF NOT EXISTS patient_diagnoses (
                diagnosis_id INT AUTO_INCREMENT PRIMARY KEY,
                patient_id VARCHAR(50),
                diagnosis_date DATETIME,
                icd10_code VARCHAR(20),
                diagnosis_description TEXT,
                severity VARCHAR(50),
                FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE
            );
        """,
        "patient_medications": """
            CREATE TABLE IF NOT EXISTS patient_medications (
                medication_id INT AUTO_INCREMENT PRIMARY KEY,
                patient_id VARCHAR(50),
                medication_name VARCHAR(255),
                dosage VARCHAR(50),
                frequency VARCHAR(50),
                start_date DATE,
                end_date DATE,
                instructions TEXT,
                FOREIGN KEY (patient_id) REFERENCES patient_demographics(patient_id) ON DELETE CASCADE
            );
        """
    }

    for table_name, query in TABLES.items():
        cursor.execute(query)
        print(f"Created table: '{table_name}'.")

    cursor.close()

def main():
    """Main function to run the database setup process."""
    conn = connect_to_mysql()
    if conn:
        create_database(conn)
        conn.close()

    db_conn = connect_to_database()
    if db_conn:
        create_tables(db_conn)
        db_conn.close()
        print("Database setup completed successfully!")

if __name__ == "__main__":
    main()
