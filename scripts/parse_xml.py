import os
import mysql.connector
from lxml import etree
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
    if patient_role is None:
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
    dob = dob_elem.get("value") if dob_elem is not None else None
    
    if dob:
        try:
            dob_datetime = datetime.strptime(dob, "%Y%m%d")
            dob = dob_datetime.strftime("%Y-%m-%d")
        except ValueError:
            dob = None
    
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
    language_codes = root.xpath(".//ns0:languageCommunication/ns0:languageCode/@code", namespaces=ns)
    language = ", ".join(sorted(set(code.lower() for code in language_codes))) if language_codes else "N/A"

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

def parse_hospitalization_date(date_str):
    """Convert CCDA date string to Python datetime object"""
    if not date_str:
        return None
    
    try:
        if len(date_str) >= 8:
            dt = datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
            if len(date_str) > 14:
                offset = date_str[14:]
                hours = int(offset[:3])
                minutes = int(offset[3:])
                dt = dt - timedelta(hours=hours, minutes=minutes)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return None
    except ValueError:
        return None

def parse_hospitalizations(root):
    """Extracts hospitalization records from CCDA XML."""
    ns = {'ns0': 'urn:hl7-org:v3'}
    hospitalizations = []

    # Find Encounters section
    section = root.xpath(
        ".//ns0:section[ns0:code[@code='46240-8']]",
        namespaces=ns
    )

    if not section:
        return hospitalizations

    # Process each entry in the section
    for entry in section[0].xpath(".//ns0:entry/ns0:encounter", namespaces=ns):
        hospitalization = {
            "encounter_type": "Unknown",
            "location": "Unknown",
            "admission_date": None,
            "discharge_date": None,
            "diagnoses": [],
            "providers": [],
            "informants": []
        }

        # Extract encounter type
        code = entry.xpath(".//ns0:code/@displayName", namespaces=ns)
        if code:
            hospitalization["encounter_type"] = code[0].strip()

        # Extract effective time (admission/discharge dates)
        effective_time = entry.xpath(".//ns0:effectiveTime", namespaces=ns)
        if effective_time:
            low = effective_time[0].xpath(".//ns0:low/@value", namespaces=ns)
            high = effective_time[0].xpath(".//ns0:high/@value", namespaces=ns)
            if low:
                hospitalization["admission_date"] = parse_hospitalization_date(low[0])
            if high:
                hospitalization["discharge_date"] = parse_hospitalization_date(high[0])

        # Extract diagnoses
        for ref in entry.xpath(".//ns0:entryRelationship/ns0:act/ns0:entryRelationship/ns0:observation/ns0:value/@displayName", namespaces=ns):
            hospitalization["diagnoses"].append(ref)
        
        # Extract Location
        location = entry.xpath(".//ns0:participant/ns0:participantRole/ns0:playingEntity/ns0:name", namespaces=ns)
        if location:
          hospitalization["location"] = location[0].text.strip()

        # Extract providers
        providers = entry.xpath(".//ns0:performer/ns0:assignedEntity/ns0:representedOrganization/ns0:name", namespaces=ns)
        for provider in providers:
            hospitalization["providers"].append(provider.text.strip())
            
        #Extract Informants
        informants = entry.xpath(".//ns0:informant/ns0:assignedEntity/ns0:representedOrganization/ns0:name", namespaces=ns)
        for informant in informants:
            hospitalization["informants"].append(informant.text.strip())
            
        hospitalizations.append(hospitalization)

    return hospitalizations

def parse_diagnoses(root):
    """Parses patient diagnoses details from XML."""
    ns = {'ns0': 'urn:hl7-org:v3'}
    diagnoses = []
    
    # Find problem section using template ID
    problem_section = root.xpath(
        ".//ns0:section[ns0:templateId[@root='2.16.840.1.113883.10.20.22.2.5.1']]", 
        namespaces=ns
    )
    
    if not problem_section:
        return diagnoses
    
    # Find table body
    tbody = problem_section[0].xpath(".//ns0:tbody", namespaces=ns)
    if not tbody:
        return diagnoses
    
    # Process each table row
    for tr in tbody[0].xpath(".//ns0:tr", namespaces=ns):
        problem_id = tr.get("ID")
        if not problem_id or not re.match(r'problem-\d+', problem_id):
            continue
            
        # Extract problem name
        problem_name = None
        for td in tr.xpath(".//ns0:td", namespaces=ns):
            content = td.xpath(f".//ns0:content[@ID='{problem_id}-problem']", namespaces=ns)
            if content:
                problem_name = content[0].text.strip()
                break
        
        # Extract date
        date = None
        for td in tr.xpath(".//ns0:td", namespaces=ns):
            content = td.xpath(".//ns0:content", namespaces=ns)
            if content and content[0].text and re.match(r'\d{1,2}/\d{1,2}/\d{4}', content[0].text):
                date_str = content[0].text.strip()
                try:
                    # Convert to 24-hour format
                    dt = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
                    date = dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    date = date_str  # Fallback to original format
                break

        entry = root.xpath(f".//ns0:entry[.//ns0:text/ns0:reference[@value='#{problem_id}']]", namespaces=ns)[0]
        translation = entry.find(".//ns0:translation[@codeSystemName='ICD-10']", namespaces=ns)
    
        if problem_name and date:
            diagnoses.append({
                "diagnosis_description": problem_name,
                "diagnosis_date": date,
                "icd10_code": translation.attrib['code'] if translation is not None else None,
                "severity": translation.attrib['displayName'] if translation is not None else ''
            })
    
    return diagnoses

def parse_ccda_date(ccda_date):
    """Convert CCDA date string to MySQL datetime format"""
    if not ccda_date:
        return None
    
    try:
        if len(ccda_date) >= 8:
            dt = datetime.strptime(ccda_date[:14], "%Y%m%d%H%M%S")
            if len(ccda_date) > 14:
                offset = ccda_date[14:]
                hours = int(offset[:3])
                minutes = int(offset[3:])
                dt = dt - timedelta(hours=hours, minutes=minutes)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return None
    except ValueError:
        return None

def extract_dosage(medication_name):
    """Extracts dosage from medication name using regex."""
    if not medication_name or not isinstance(medication_name, str):
        return None, None  # Return None for both if name is invalid

    # Regex to find dosage (number + units like mg, mcg, etc.)
    dosage_match = re.search(r"(\d*\.?\d+(?:\s*[A-Za-z]+)(?:/\s*[A-Za-z]+)?)", medication_name)

    if dosage_match:
        dosage = dosage_match.group(1)
        cleaned_name = medication_name.replace(dosage, "").strip()
        cleaned_name = " ".join(cleaned_name.split())
        dosage = re.sub(r"(\d*\.?\d+)([A-Za-z]+)", r"\1 \2", dosage)

        return dosage, cleaned_name
    else:
        return None, medication_name

def parse_medications(root):
    """Parses patient medications details from XML."""
    ns = {'ns0': 'urn:hl7-org:v3'}
    medications = []

    # Locate the Medications section (LOINC code 10160-0)
    section = root.xpath(".//ns0:section[ns0:code[@code='10160-0']]", namespaces=ns)
    if not section:
        return medications  # No medications section found

    for entry in section[0].xpath(".//ns0:entry", namespaces=ns):
        substance_adm = entry.find(".//ns0:substanceAdministration", namespaces=ns)
        if substance_adm is not None:
            medication = {}

            # Extract Medication Name and dosages
            name_elem = substance_adm.find(".//ns0:manufacturedMaterial/ns0:code", namespaces=ns)
            medication_name = medication["medication_name"] = name_elem.get("displayName") if name_elem is not None else "Unknown"

            if medication_name is None:
                continue

            dosage, cleaned_name = extract_dosage(medication_name)
            medication["medication_name"] = cleaned_name.title() or "Unknown"  # Use cleaned name if available, else Unknown
            medication["dosage"] = dosage

            # Extract Duration (Start)
            start_elem = substance_adm.find(".//ns0:effectiveTime/ns0:low", namespaces=ns)
            medication["start_date"] = parse_ccda_date(start_elem.get("value")) if start_elem is not None else None

            # Extract Instructions from <entryRelationship typeCode="SUBJ" inversionInd="true">
            instruction_elem = entry.find(".//ns0:entryRelationship/ns0:act/ns0:text", namespaces=ns)
            medication["instructions"] = instruction_elem.text.capitalize() if instruction_elem is not None else "No specific instructions"

            medications.append(medication)

    return medications


def insert_patient_data(patient_data):
    """Inserts validated patient data into MySQL."""
    conn = connect_to_db()
    cursor = conn.cursor()

    print(f"Adding patient's demographics data for id: {patient_data['patient_id']}...")

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

    print(f"Adding hospitalization records for patient with id: {patient_id}")

    conn = connect_to_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO patient_hospitalizations (
            patient_id, 
            admission_date, 
            discharge_date, 
            hospital_name, 
        )
        VALUES (%s, %s, %s, %s)
    """
    try:
        for record in hospitalizations:
            try:
                cursor.execute(sql, (
                    patient_id,
                    record.get('admission_date'),
                    record.get('discharge_date'),
                    record.get('hospital_name'),
                ))
            except Exception as inner_e:
                print(f"Error inserting record: {record}. Error: {inner_e}")
                conn.rollback()  # Rollback if one record fails
        conn.commit()  # Commit after all records are processed
        print(f"Successfully added hospitalization records for patient with id: {patient_id}")
    except Exception as outer_e:
        print(f"Database insertion failed: {outer_e}")
        conn.rollback()  # Rollback if outer loop fails
    finally:
        cursor.close()
        conn.close()

def insert_diagnoses(diagnoses, patient_id):
    """Inserts diagnoses data into MySQL."""
    conn = connect_to_db()
    cursor = conn.cursor()

    print(f"Adding diagnoses for patient with id: {patient_id}")

    sql = """
        INSERT INTO patient_diagnoses (patient_id, diagnosis_date, icd10_code, diagnosis_description, severity)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            diagnosis_description=VALUES(diagnosis_description), 
            severity=VALUES(severity);
    """
    try:
        for record in diagnoses:
            # Unpack record values and add patient_id as first parameter
            cursor.execute(sql, (
                patient_id,
                record['diagnosis_date'],
                record['icd10_code'],
                record['diagnosis_description'],
                record['severity']
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database insertion failed: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()

def insert_medications(medications, patient_id):
    """Inserts medication data into MySQL."""

    print(f"Adding medications' record for patient with id: {patient_id}")

    conn = connect_to_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO patient_medications (
            medication_id, 
            patient_id, 
            medication_name, 
            dosage, 
            frequency, 
            start_date, 
            instructions
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            medication_name=VALUES(medication_name),
            dosage=VALUES(dosage), 
            start_date=VALUES(start_date), 
            instructions=VALUES(instructions);"""
    try:
        for record in medications:
            cursor.execute(sql, (
                None,  # Auto-incrementing medication_id
                patient_id,
                record['medication_name'],
                record['dosage'],
                record['start_date'],
                record['instructions']
            ))
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database insertion failed: {str(e)}")
    
    finally:
        cursor.close()
        conn.close()

def process_xml_files():
    """Processes all XML files in the 'incoming' folder."""
    xml_folder = "data/incoming/"
    for filename in os.listdir(xml_folder):
        if filename.endswith(".xml"):
            xml_path = os.path.join(xml_folder, filename)
            print(f"Processing file: {filename}\n")

            tree = etree.parse(xml_path)
            root = tree.getroot()

            patient_data = parse_patient_data(root)
            hospitalizations = parse_hospitalizations(root)
            diagnoses = parse_diagnoses(root)
            medications = parse_medications(root)

            for meds in medications:
                print(meds)

            if patient_data:
                print('Updating the database...')

                insert_patient_data(patient_data)
                insert_hospitalizations(hospitalizations, patient_data["patient_id"])
                insert_diagnoses(diagnoses, patient_data["patient_id"])
                insert_medications(medications, patient_data["patient_id"])

                os.rename(xml_path, f"data/processed/{filename}")

if __name__ == "__main__":
    process_xml_files()
