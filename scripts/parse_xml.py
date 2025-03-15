import os
import mysql.connector
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load database credentials
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = "medical_data"

def connect_to_db():
    """Establishes a connection to MySQL."""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def extract_text(element):
    """Returns text content from an XML element, or None if missing."""
    return element.text.strip() if element is not None and element.text else None

def map_gender_code(gender_code):
    gender_map = {"M": "Male", "F": "Female", "O": "Other", "U": "Unknown"}
    return gender_map.get(gender_code, "Unknown")

def parse_patient_data(xml_file):
    """Parses patient demographic details from XML."""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
     # Extract patient ID (first valid one found)
    patient_id = None
    for id_element in root.findall(".//{urn:hl7-org:v3}id"):
        if "extension" in id_element.attrib:
            patient_id = id_element.attrib["extension"]
            break

    first_name = extract_text(root.find(".//{urn:hl7-org:v3}given"))
    last_name = extract_text(root.find(".//{urn:hl7-org:v3}family"))
    birth_date = root.find(".//{urn:hl7-org:v3}birthTime").attrib.get("value") if root.find(".//{urn:hl7-org:v3}birthTime") is not None else None
    gender_code = root.find(".//{urn:hl7-org:v3}translation").attrib.get("code") if root.find(".//{urn:hl7-org:v3}translation") is not None else None
    gender = map_gender_code(gender_code)

    # Extract address details
    addr = root.find(".//{urn:hl7-org:v3}addr")
    street = extract_text(addr.find("{urn:hl7-org:v3}streetAddressLine")) if addr is not None else None
    city = extract_text(addr.find("{urn:hl7-org:v3}city")) if addr is not None else None
    state = extract_text(addr.find("{urn:hl7-org:v3}state")) if addr is not None else None
    postal_code = extract_text(addr.find("{urn:hl7-org:v3}postalCode")) if addr is not None else None

    # Extract contact details
    contact = {"home_phone": None, "mobile_phone": None, "email": None}
    for telecom in root.findall(".//{urn:hl7-org:v3}telecom"):
        value = telecom.attrib.get("value", "").replace("tel:", "").replace("mailto:", "").strip()
        if "@" in value:
            contact["email"] = value
        elif value.startswith("+") or value.isdigit():
            if "home" in telecom.attrib.get("use", "").lower():
                contact["home_phone"] = value
            else:
                contact["mobile_phone"] = value

    # Extract race and ethnicity
    race = root.find(".//{urn:hl7-org:v3}raceCode")
    ethnicity = root.find(".//{urn:hl7-org:v3}ethnicGroupCode")
    race = race.attrib.get("displayName", "Unknown") if race is not None else "Unknown"
    ethnicity = ethnicity.attrib.get("displayName", "Unknown") if ethnicity is not None else "Unknown"

    return {
        "patient_id": patient_id,
        "first_name": first_name,
        "last_name": last_name,
        "birth_date": birth_date,
        "gender": gender,
        "street": street,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "home_phone": contact["home_phone"],
        "mobile_phone": contact["mobile_phone"],
        "email": contact["email"],
        "race": race,
        "ethnicity": ethnicity
    }

def insert_patient_data(patient_data):
    """Inserts validated patient data into MySQL."""
    conn = connect_to_db()
    cursor = conn.cursor()

    sql = """
        INSERT INTO patient_demographics (patient_id, first_name, last_name, DOB, gender, street, city, state, postal_code, home_phone, mobile_phone, email, race, ethnicity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            first_name=VALUES(first_name), 
            last_name=VALUES(last_name), 
            DOB=VALUES(birth_date), 
            gender=VALUES(gender),
            street=VALUES(street),
            city=VALUES(city),
            state=VALUES(state),
            postal_code=VALUES(postal_code),
            home_phone=VALUES(home_phone),
            mobile_phone=VALUES(mobile_phone),
            email=VALUES(email),
            race=VALUES(race),
            ethnicity=VALUES(ethnicity);
    """
    values = tuple(patient_data.values())
    cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Patient {patient_data['patient_id']} updated successfully.")

def process_xml_files():
    """Processes all XML files in the 'incoming' folder."""
    xml_folder = "data/incoming/"
    for filename in os.listdir(xml_folder):
        if filename.endswith(".xml"):
            xml_path = os.path.join(xml_folder, filename)
            print(f"Processing file: {filename}\n")
            patient_data = parse_patient_data(xml_path)

            if patient_data:
                insert_patient_data(patient_data)
                os.rename(xml_path, f"data/processed/{filename}")

if __name__ == "__main__":
    process_xml_files()
