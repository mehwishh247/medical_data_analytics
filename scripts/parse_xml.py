import os
import mysql.connector
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import datetime

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

def parse_patient_data(root):
    """Parses patient demographic details from XML."""
    
     # Extract patient ID (first valid one found)
    patient_id = None
    for id_element in root.findall(".//{urn:hl7-org:v3}id"):
        if "extension" in id_element.attrib:
            patient_id = id_element.attrib["extension"]
            break

    first_name = extract_text(root.find(".//{urn:hl7-org:v3}given"))
    last_name = extract_text(root.find(".//{urn:hl7-org:v3}family"))
    
    dob_raw = extract_text(root.find(".//{urn:hl7-org:v3}birthTime"))
    DOB = None
    if dob_raw:
        try:
            DOB = datetime.strptime(dob_raw[:8], "%Y%m%d").strftime("%Y-%m-%d")  # Extract YYYYMMDD and convert
        except ValueError:
            DOB = None  # Set to NULL if format is invalid

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
        "DOB": DOB,
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

def parse_hospitalizations(root):
    """Parses patient hospitalization details from XML."""
    hospitalizations = []
    for encounter in root.findall(".//{urn:hl7-org:v3}encounter"): 
        hospitalization_id_elem = encounter.find(".//{urn:hl7-org:v3}id")
        hospitalization_id = hospitalization_id_elem.attrib.get("extension") if hospitalization_id_elem is not None else None
        
        if hospitalization_id is None:
            continue  # Skip records with missing hospitalization_id

        admission_date = extract_text(encounter.find(".//{urn:hl7-org:v3}effectiveTime/{urn:hl7-org:v3}low"))
        discharge_date = extract_text(encounter.find(".//{urn:hl7-org:v3}effectiveTime/{urn:hl7-org:v3}high"))
        hospital_name = extract_text(encounter.find(".//{urn:hl7-org:v3}name"))
        service_details = extract_text(encounter.find(".//{urn:hl7-org:v3}code"))

        hospitalizations.append((hospitalization_id, admission_date, discharge_date, hospital_name, service_details))
    return hospitalizations

def parse_diagnoses(root):
    """Parses patient diagnoses details from XML."""
    diagnoses = []
    for entry in root.findall(".//{urn:hl7-org:v3}observation"):
        diagnosis_date_raw = extract_text(entry.find(".//{urn:hl7-org:v3}effectiveTime"))
        
        # Convert to MySQL DATETIME format or set to None
        diagnosis_date = None
        if diagnosis_date_raw:
            try:
                diagnosis_date = datetime.strptime(diagnosis_date_raw, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                diagnosis_date = None  # Set to None if parsing fails
        
        icd10_code = extract_text(entry.find(".//{urn:hl7-org:v3}code"))
        diagnosis_description = extract_text(entry.find(".//{urn:hl7-org:v3}text"))
        severity = extract_text(entry.find(".//{urn:hl7-org:v3}interpretationCode"))

        diagnoses.append((diagnosis_date, icd10_code, diagnosis_description, severity))
    
    return diagnoses

def parse_medications(root):
    """Parses patient medications details from XML."""
    medications = []
    for med in root.findall(".//{urn:hl7-org:v3}substanceAdministration"): 
        medication_name = extract_text(med.find(".//{urn:hl7-org:v3}manufacturedProduct/{urn:hl7-org:v3}name"))
        dosage = extract_text(med.find(".//{urn:hl7-org:v3}doseQuantity"))
        frequency = extract_text(med.find(".//{urn:hl7-org:v3}rateQuantity"))
        start_date = extract_text(med.find(".//{urn:hl7-org:v3}effectiveTime/{urn:hl7-org:v3}low"))
        end_date = extract_text(med.find(".//{urn:hl7-org:v3}effectiveTime/{urn:hl7-org:v3}high"))
        medications.append((medication_name, dosage, frequency, start_date, end_date))
    return medications

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
            DOB=VALUES(DOB), 
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

def insert_hospitalizations(hospitalizations, patient_id):
    """Inserts hospitalization data into MySQL."""
    conn = connect_to_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO patient_hospitalizations (patient_id, hospitalization_id, admission_date, discharge_date, hospital_name, service_details)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            admission_date=VALUES(admission_date), 
            discharge_date=VALUES(discharge_date), 
            hospital_name=VALUES(hospital_name),
            service_details=VALUES(service_details);
    """
    for record in hospitalizations:
        cursor.execute(sql, (patient_id, *record))
    conn.commit()
    cursor.close()
    conn.close()

def insert_diagnoses(diagnoses, patient_id):
    """Inserts diagnoses data into MySQL."""
    conn = connect_to_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO patient_diagnoses (patient_id, diagnosis_date, icd10_code, diagnosis_description, severity)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            diagnosis_description=VALUES(diagnosis_description), 
            severity=VALUES(severity);
    """
    for record in diagnoses:
        cursor.execute(sql, (patient_id, *record))
    conn.commit()
    cursor.close()
    conn.close()

def insert_medications(medications, patient_id):
    """Inserts medication data into MySQL."""
    conn = connect_to_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO patient_medications (patient_id, medication_name, dosage, frequency, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            dosage=VALUES(dosage), 
            frequency=VALUES(frequency), 
            start_date=VALUES(start_date), 
            end_date=VALUES(end_date);
    """
    for record in medications:
        cursor.execute(sql, (patient_id, *record))
    conn.commit()
    cursor.close()
    conn.close()

def process_xml_files():
    """Processes all XML files in the 'incoming' folder."""
    xml_folder = "data/incoming/"
    for filename in os.listdir(xml_folder):
        if filename.endswith(".xml"):
            xml_path = os.path.join(xml_folder, filename)
            print(f"Processing file: {filename}\n")

            tree = ET.parse(xml_path)
            root = tree.getroot()

            patient_data = parse_patient_data(root)
            hospitalizations = parse_hospitalizations(root)
            diagnoses = parse_diagnoses(root)
            medications = parse_medications(root)

            if patient_data:
                insert_patient_data(patient_data)
                insert_hospitalizations(hospitalizations, patient_data["patient_id"])
                insert_diagnoses(diagnoses, patient_data["patient_id"])
                insert_medications(medications, patient_data["patient_id"])

                os.rename(xml_path, f"data/processed/{filename}")

if __name__ == "__main__":
    process_xml_files()
