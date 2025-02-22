import pandas as pd
import requests
import time
import timeit
import psycopg2 as py
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

#DB CREDENTIALS

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def affilation_id_extraction():
    data = []
    for state_id in range(16,17):   #2 is the state id 

        url = "https://saras.cbse.gov.in/SARAS/AffiliatedList/ListOfSchdirReport"
        payload ={
        'State': state_id,
        'SchoolStatusWise': '0',
        'InstName_orAddress': '',
        '__RequestVerificationToken': 'CfDJ8HnbQzDxziZGntZLN4S1VYaaGZoB5U5dV8HfJRSd7xnksrRmPm2JRied9rXo8FiUUK7y7_0t2RY9kNJGQ3xfcdta1CGO_cbSDKcfzl8L8H1hM51_1ObiuvNJfIM2iL6zRcVWnFr1nI4to24X-5l-M3k'
        }

        response = requests.post(url, data=payload)
        soup = BeautifulSoup(response.content, 'html.parser')
        rows = soup.find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            if len(cols) > 1:
                data.append(cols[1].text.strip())

        df = pd.DataFrame(data, columns=['Affiliation Number'])
        df.to_csv('affiliation_ids.csv', index=False)

        print(df)
        extract_school_data()


def extract_school_data_by_affiliation_id(affiliation_id,session):
    """_summary_

    Args:
        affiliation_id (INT): TO PASS THE AFFILIATION ID 
        session (_type_): TO PASS THE SESSION REQUESTS

    Returns:
        _type_: _description_
    """
    try:
        affiliation_id = int(affiliation_id)
        url = f"https://saras.cbse.gov.in/SARAS/AffiliatedList/AfflicationDetails/{affiliation_id}"
        response = session.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"class": "table table-bordered"})
        table_data = {}

        for row in table.find_all("tr"):
            cols = row.find_all("td")

            if len(cols) == 0:
                continue

            table_data[cols[0].text.strip()] = cols[1].text.strip()  
        return table_data
    
    except Exception as e:
        print(f"Error fetching data for affiliation ID {affiliation_id}: {e}")
        return None



def extract_school_data():

    print("Extracting School Data...")
    cbse_data = []
    failed_schools = []  # List to store failed schools

    df = pd.read_csv("affiliation_ids.csv")
    session = requests.Session()

    for index, row in df.iterrows():
        try:
            print(f"Processing Affiliation Number :{row['Affiliation Number']}")
            school_data=extract_school_data_by_affiliation_id(row["Affiliation Number"],session)
            if school_data:
                cbse_data.append(school_data)
            else:
                failed_schools.append(row["Affiliation Number"])
        except Exception as e:
                print(f"Error processing affiliation number {row['Affiliation Number']}: {e}")
                failed_schools.append(row["Affiliation Number"])


    newdf = pd.DataFrame(cbse_data)
    #print(newdf.to_string())
    newdf.to_csv("cbse_data_extracted.csv", index=False)

    
    move_data_to_db(newdf,failed_schools)


def extract_dates(affiliation_period):
    """Extract 'from' and 'to' dates using regex."""
    date_pattern = r"From\s*:\s*(\d{2}/\d{2}/\d{4})\s*To\s*:\s*(\d{2}/\d{2}/\d{4})"
    match = re.search(date_pattern, affiliation_period)
    if match:
        affiliation_from = match.group(1)
        affiliation_to = match.group(2)

        # Convert to 'YYYY-MM-DD' format
        try:
            affiliation_from = datetime.strptime(affiliation_from, '%d/%m/%Y').strftime('%Y-%m-%d')
            affiliation_to = datetime.strptime(affiliation_to, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError as ve:
            print(f"Error parsing dates: {ve}")
            return None, None
        
        return affiliation_from, affiliation_to
    return None, None


def extract_experience_from_text(text):
    """Extract integer values from the text using regex."""
    match = re.search(r'\d+', text)
    return int(match.group()) if match else 0


def move_data_to_db(newdf,failed_schools):
    """_summary_

    Args:
        newdf (DATAFRAME): TO ITERATE THE ROWS
    """    
    try: 
        conn = py.connect(
            dbname= DB_NAME,
            user= DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        cur = conn.cursor()


        for i, row in newdf.iterrows():
            try:


                affiliation_from,affiliation_to = extract_dates(row['Affiliation Period'])

                administrative_exp = extract_experience_from_text(row['Administrative:'])
                teaching_exp = extract_experience_from_text(row['Teaching:'])

                print(i)
                cur.execute(
                    """
                    INSERT INTO cbse.cbse_data (
                        institution_name,
                        affiliation_number,
                        state_name,
                        district,
                        postal_address,
                        pincode,
                        website,
                        year_of_foundation,
                        date_of_first_opening_of_school,
                        name_of_principal_or_head_of_institution,
                        gender,
                        principal_educational,
                        administrative_experience,
                        teaching_experience,
                        school_status,
                        affiliation_type,
                        affiliation_period_from,
                        affiliation_period_to,
                        name_of_trust ,
                        updated_at                     
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'now()')
                    ON CONFLICT (affiliation_number) DO UPDATE
                    SET
                        institution_name = EXCLUDED.institution_name,
                        state_name = EXCLUDED.state_name,
                        district = EXCLUDED.district,
                        postal_address = EXCLUDED.postal_address,
                        pincode = EXCLUDED.pincode,
                        website = EXCLUDED.website,
                        year_of_foundation = EXCLUDED.year_of_foundation,
                        date_of_first_opening_of_school = EXCLUDED.date_of_first_opening_of_school,
                        name_of_principal_or_head_of_institution = EXCLUDED.name_of_principal_or_head_of_institution,
                        gender = EXCLUDED.gender,
                        principal_educational = EXCLUDED.principal_educational,
                        administrative_experience = EXCLUDED.administrative_experience,
                        teaching_experience = EXCLUDED.teaching_experience,
                        school_status = EXCLUDED.school_status,
                        affiliation_type = EXCLUDED.affiliation_type,
                        affiliation_period_from = EXCLUDED.affiliation_period_from,
                        affiliation_period_to = EXCLUDED.affiliation_period_to,                        
                        name_of_trust = EXCLUDED.name_of_trust,
                        updated_at = now()
                    """, (
                        row['Name of Institution'] or None,  
                        row['Affiliation Number'] ,
                        row['State'] or None,
                        row['District'] or None,
                        row['Postal Address'] or None,
                        row['Pin Code'] or None,
                        row['Website'] or None,
                        row['Year of Foundation'] or None,
                        row['Date of First Opening of School'] or None,
                        row['Name of Principal/ Head of Institution'] or None,
                        row['Gender'] or None,
                        row["Principal's Educational/Professional Qualifications:"] or None,
                        administrative_exp or None,
                        teaching_exp or None,
                        row['Status of The School'] or None,
                        row['Type of affiliation'] or None,
                        affiliation_from or None,  
                        affiliation_to or None,    
                        row['Name of Trust/ Society/ Managing Committee'] or None
                    )
                )
                conn.commit()
            except Exception as e:
                print(f"Error inserting/updating affiliation number {row['Affiliation Number']}: {e}")
                conn.rollback()
                failed_schools.append(row['Affiliation Number'])
        
        print("Succesfully added the Data into Database")   
        conn.commit()
        cur.close()
        conn.close()


        if failed_schools:
            print(f"\nThe following schools failed during data insertion: {failed_schools}")

    except Exception as e:
        print(f"Error moving data to database: {e}")




if __name__ == "__main__":
    start = timeit.default_timer()
    print("Starting...")
    affilation_id_extraction()
    stop = timeit.default_timer()
    print(f"Time taken: {stop - start} Seconds")