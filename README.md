# Instructions

## Prerequisites installations

Before setting up the project, ensure the following dependencies are installed:

- Python 3.8 or later
- MySQL Server
- pip (Python package manager)
- Virtual environment (optional but recommended)


## Step 1: Clone the Repository

Navigate to the directory where you want to store the project and clone the repository:

```bash
git clone https://github.com/mehwishh247/medical_data_analytics
cd medical_data_analytics
```

## Step 2: Create and Activate a Virtual Environment (Optional)
```
python3 -m venv .venv
source .venv/bin/activate  # On macOS/Linux
.venv\Scripts\activate    # On Windows

```

## Step 3: Install Dependencies

Install the required Python packages:

>> pip install -r requirements.txt

## Step 4: Configure Environment Variables

Create a .env file in the project root and add the following details:
```
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=yourpassword
DB_NAME=medical_data
```

## Step 6: Place XML Files

- Move XML files to the data/incoming/ directory for processing
- Once the files are processed, they will automatically move to data/processed folder

## Step 7: Run the Data Processing Script

To parse XML files and insert data into the database, run:

>> python3 scripts/parse_xml.py

## Step 8: Verify Data in MySQL

Connect to MySQL and check if data has been inserted correctly:

```
USE medical_data;
SELECT * FROM patient_demographics;
SELECT * FROM patient_hospitalizations;
SELECT * FROM patient_diagnoses;
SELECT * FROM patient_medications;
```


