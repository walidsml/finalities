import tabula
import pandas as pd
import re
import pymysql.cursors
from datetime import datetime
from fuzzywuzzy import process
import csv

# Database connection settings
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'SomathesProducts',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_uid_from_database(code_article):
    # Function to get UID from SQL database based on EAN (Code Article)
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT UID FROM Products WHERE EAN = %s"
            cursor.execute(sql, (code_article,))
            result = cursor.fetchone()
            return result['UID'] if result else None
    finally:
        connection.close()

def get_database_entries():
    # Function to fetch all entries (Den and UID) from the clients table
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT UID, Den FROM clients"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()

def find_closest_match(input_text, entries):
    # Function to find the closest match for store name and city name
    input_store, input_city = split_store_city(input_text)
    
    # Remove specific words from input store name
    words_to_remove = ["market", "medina"]
    for word in words_to_remove:
        input_store = re.sub(r'\b{}\b'.format(word), '', input_store, flags=re.IGNORECASE).strip()
    
    best_match = None
    highest_score = 0

    for entry in entries:
        db_store, db_city = split_store_city(entry['Den'])
        
        # Calculate the score for both store and city
        store_score = process.extractOne(input_store, [db_store])[1]
        city_score = process.extractOne(input_city, [db_city])[1]
        
        # Average the scores
        score = (store_score + city_score) / 2
        
        if score > highest_score:
            highest_score = score
            best_match = entry
    
    return best_match['Den'], best_match['UID'], highest_score

def split_store_city(text):
    # Function to split store name and city name from input text
    parts = re.split(r'\s+', text, maxsplit=1)
    store = parts[0]
    city = parts[1] if len(parts) > 1 else ''
    return store, city
                        
# Path to your PDF file
pdf_path = "templatefull2.pdf"

try:
    # Extract tables from the PDF
    tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, encoding='latin1')
    if tables:
        # Assuming the first table is the product details
        product_details = tables[0]

        # Map Code Article to UID
        product_details['Code Article'] = product_details['Code Article'].apply(lambda x: get_uid_from_database(x))

        # Extracting raw text from the PDF for order details
        order_text = tables[1].to_string(index=False)
        print("Order text extracted from PDF:\n", order_text)  # Debug print

        # Initialize the order dictionary
        order_dict = {
            "SALFCY": "AL1",
            "SOHTYP": "SON",
            "SOHNUM": "",
            "BPCORD": "",
            "ORDDAT": datetime.today().strftime('%Y%m%d'),
            "CUSORDREF": "",
            "STOFCY": "AL1",  # Set STOFCY same as SALFCY
            "EXPDATE": "",
            "ADR": "ADR",
        }

        # Define patterns for extracting Site, Commande, and Date livraison prevue from order text
        patterns = {
            "Site": r'Site\s*:\s*([^\n]+)',
            "Commande": r'Commande\s*:\s*([\d]+)',
            "Date Livraison Prevue": r'Date Livraison Prevue\s*:\s*([\d/]+)'
        }

        # Extract Site, Commande, and Date livraison prevue using regex
        for key, pattern in patterns.items():
            match = re.search(pattern, order_text)
            if match:
                order_dict[key] = match.group(1).strip()
                print(f"{key} extracted: {order_dict[key]}")  # Debug print
            else:
                print(f"{key} not found")  # Debug print

        # Remove specific words from site name if present
        words_to_remove = ["market", "medina"]
        for word in words_to_remove:
            order_dict["Site"] = re.sub(r'\b{}\b'.format(word), '', order_dict["Site"], flags=re.IGNORECASE).strip()

        # Find the closest match for Site and get its UID
        entries = get_database_entries()
        order_dict["BPCORD"] = find_closest_match(order_dict["Site"], entries)[1]

        # Set CUSORDREF from Commande
        order_dict["CUSORDREF"] = order_dict["Commande"]

        # Format the Date Livraison Prevue to the desired format if it was found
        if "Date Livraison Prevue" in order_dict:
            delivery_date = datetime.strptime(order_dict["Date Livraison Prevue"], '%d/%m/%Y').strftime('%Y%m%d')
            order_dict["EXPDATE"] = delivery_date
        else:
            order_dict["EXPDATE"] = "error date livraison prevue"  # Handle missing date

        # Prepare header and lines for Sage ERP CSV
        header = [
            'E',
            order_dict['SALFCY'],
            order_dict['SOHTYP'],
            order_dict['SOHNUM'],
            order_dict['BPCORD'],
            order_dict['ORDDAT'],
            order_dict['CUSORDREF'],
            order_dict['STOFCY'],
            order_dict['EXPDATE'],
            order_dict['ADR'],
        ]

        lines = []
        for _, row in product_details.iterrows():
            line = [
                'L',
                row['Code Article'],
                '',
                'CAR',  # SAU before QTY
                row['Qte Cmd UA'],
                '',
                '',
                '',
                '',
                ''
            ]
            lines.append(line)

        # Write to CSV
        output_file = "/Users/walid/Desktop/finalities/sage_erp.csv"
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(header)
            writer.writerows(lines)

        print(f"Sage ERP CSV created successfully at {output_file}")

    else:
        print("No tables found in the PDF.")

except Exception as e:
    print(f"Error reading PDF: {e}")
