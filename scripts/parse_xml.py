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
    ns = {"ns0": "urn:hl7-org:v3", "ns2": "urn:hl7-org:sdtc"}
    patient_role = root.find(".//{urn:hl7-org:v3}patientRole")
    if not patient_role:
        return {}
    
    patient_id_elem = root.find(".//ns0:id", ns)
    patient_id = patient_id_elem.get("extension") if patient_id_elem is not None else "N/A"
    
    patient_role = root.find(".//ns0:recordTarget/ns0:patientRole", ns)
    patient = patient_role.find("ns0:patient", ns) if patient_role is not None else None
    
    given_names = patient.findall("ns0:name/ns0:given", ns) if patient is not None else []
    family_name = patient.findtext("ns0:name/ns0:family", "", ns) if patient is not None else ""
    first_name = " ".join([name.text for name in given_names if name.text]).strip()
    last_name = family_name.strip()
    
    dob_elem = patient.find("ns0:birthTime", ns) if patient is not None else None
    dob = dob_elem.get("value") if dob_elem is not None else "N/A"
    
    gender_elem = root.find(".//ns0:patient/ns0:administrativeGenderCode/ns0:translation", ns)
    gender = gender_elem.get("displayName") if gender_elem is not None else "N/A"
    
    race_elem = root.find(".//ns0:patient/ns0:raceCode/ns0:translation", ns)
    race = race_elem.get("displayName") if race_elem is not None else "N/A"
    
    ethnicity_elem = root.find(".//ns0:patient/ns0:ethnicGroupCode/ns0:translation", ns)
    ethnicity = ethnicity_elem.get("displayName") if ethnicity_elem is not None else "N/A"
    
    marital_status_elem = root.find(".//ns0:patient/ns0:maritalStatusCode/ns0:translation", ns)
    marital_status = marital_status_elem.get("displayName") if marital_status_elem is not None else "N/A"
    
    address_elem = patient_role.find("ns0:addr", ns) if patient_role is not None else None
    address_parts = [
        address_elem.findtext("ns0:streetAddressLine", "", ns),
        address_elem.findtext("ns0:city", "", ns),
        address_elem.findtext("ns0:state", "", ns),
        address_elem.findtext("ns0:postalCode", "", ns)
    ] if address_elem is not None else []
    address = ", ".join(filter(None, address_parts)) if address_parts else "N/A"
    
    home_phone, mobile_phone, email = "None", "None", "None"
    for telecom in root.findall(".//ns0:patientRole/ns0:telecom", ns):
        telecom_value = telecom.get("value", "").replace("tel:", "").replace("mailto:", "").strip()
        telecom_use = telecom.get("use", "").strip().lower()
        if "hp" in telecom_use:  # Home phone
            home_phone = telecom_value if telecom_value.lower() not in ["none", "null"] else "None"
        elif "mc" in telecom_use:  # Mobile phone
            mobile_phone = telecom_value if telecom_value.lower() not in ["none", "null"] else "None"
        elif "h" in telecom_use and "mailto" in telecom_value:  # Email
            email = telecom_value if telecom_value.lower() not in ["none", "null"] else "None"
    
    # Languages (Unique list)
    language = [lang.get("code", "N/A") for lang in root.findall(".//ns0:patient/ns0:languageCommunication/ns0:languageCode", ns)]
    language = ", ".join(language) if language else "N/A"

    return {
        "patient_id": patient_id,
        "first_name": first_name,
        "Last Name": last_name,
        "DOB": dob,
        "Gender": gender,
        "Race": race,
        "Ethnicity": ethnicity,
        "Marital Status": marital_status,
        "Address": address,
        "Home Phone": home_phone,
        "Mobile Phone": mobile_phone,
        "Email": email,
        "Language": language
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

    diagnoses_list = root.find('''.//*[@code="11450-4"]''')
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
        INSERT INTO patient_demographics (patient_id, first_name, last_name, DOB, gender, race, ethnicity, marital_status, address, home_phone, mobile_phone, email, language)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            patient_id=VALUES(patient_id),
            first_name=VALUES(first_name), 
            last_name=VALUES(last_name), 
            DOB=VALUES(DOB), 
            gender=VALUES(gender),
            race=VALUES(race),
            ethnicity=VALUES(ethnicity),
            marital_status=VALUES(marital_status),
            address=VALUES(Address),
            home_phone=VALUES(home_phone),
            mobile_phone=VALUES(mobile_phone),
            email=VALUES(email),
            language=VALUES(language);
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
