import tabula
import os
import pandas as pd
import pymysql.cursors
from fuzzywuzzy import process
import re
from datetime import datetime

# Specify the PDF file path
pdf_file = 'templatefull3.pdf'  # Adjust this path if needed
output_dir = '/Users/walid/Desktop/result/temp22/'  # Directory to save the CSV files

# Create output directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Extract tables from the PDF using the lattice method
tables = tabula.read_pdf(pdf_file, pages='all', multiple_tables=True, lattice=True)

# Print the number of tables extracted
print(f"Total tables extracted: {len(tables)}")

# Define the expected headers and their corresponding names
expected_headers_with_names = {
    'orderer_details': ['Commande par', 'Livre a', 'Commande a'],
    'products_detail': ['Code externe', 'Code EAN', 'Libelle article', 'Type U.C.', 'VL', 'No ligne', 'UVC/UC', 'Quant en UC', 'No opera speci', 'ion le'],
    'order_information': ['No commande', 'Date commande', 'Code fournisseur', 'Contrat commercial', 'Filiere'],
    'delivery_details': ['Date de livraison souhaitee', 'Date de livraison limite']
}

# Normalize the expected headers by joining split lines and removing extra spaces
def normalize_header(header):
    return [' '.join(col.split()).replace('\n', ' ').replace('\r', ' ').strip() for col in header]

normalized_expected_headers_with_names = {name: normalize_header(header) for name, header in expected_headers_with_names.items()}

# Debug: print normalized expected headers
print("Normalized expected headers:")
for name, headers in normalized_expected_headers_with_names.items():
    print(f"{name}: {headers}")

# Dictionary to hold concatenated tables
concatenated_tables = {name: pd.DataFrame(columns=normalized_expected_headers_with_names[name]) for name in normalized_expected_headers_with_names.keys()}

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'SomathesProducts',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def modify_code_ean_column(table_df, cursor):
    if 'Code EAN' not in table_df.columns:
        raise ValueError("Column 'Code EAN' not found in DataFrame.")
    for index, row in table_df.iterrows():
        value = row['Code EAN']
        sql = "SELECT UID FROM products WHERE EAN = %s"
        cursor.execute(sql, (value,))
        result = cursor.fetchone()
        if result:
            uid_value = result['UID']
            table_df.at[index, 'Code EAN'] = uid_value

def get_database_entries():
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
    input_store, input_city = split_store_city(input_text)
    best_match = None
    highest_score = 0
    for entry in entries:
        db_store, db_city = split_store_city(entry['Den'])
        store_score = process.extractOne(input_store, [db_store])[1]
        city_score = process.extractOne(input_city, [db_city])[1]
        score = (store_score + city_score) / 2
        if score > highest_score:
            highest_score = score
            best_match = entry
    return best_match['Den'], best_match['UID'], highest_score

def split_store_city(text):
    parts = re.split(r'\s+', text, maxsplit=1)
    store = parts[0]
    city = parts[1] if len(parts) > 1 else ''
    return store, city

def generate_erp_sage_csv(orderer_df, order_info_df, delivery_df, products_df):
    e_lines = []
    l_lines = []

    # Define constants
    SALFCY = "AL1"
    SOHTYP = "SON"
    STOFCY = "AL1"
    adresse = "ADR"

    # Convert date columns to datetime using the correct format
    date_format = "%d/%m/%y %H:%M"
    if not pd.api.types.is_datetime64_any_dtype(order_info_df['Date commande']):
        order_info_df['Date commande'] = pd.to_datetime(order_info_df['Date commande'], format=date_format, errors='coerce')
    if not pd.api.types.is_datetime64_any_dtype(delivery_df['Date de livraison souhaitee']):
        delivery_df['Date de livraison souhaitee'] = pd.to_datetime(delivery_df['Date de livraison souhaitee'], format=date_format, errors='coerce')

    # Extract values for the E line
    for _, orderer_row in orderer_df.iterrows():
        BPCORD = orderer_row['Commande par']
        for _, order_info_row in order_info_df.iterrows():
            ORDDAT = order_info_row['Date commande'].strftime('%Y%m%d') if pd.notna(order_info_row['Date commande']) else ''
            CUSORDREF = order_info_row['No commande']
            for _, delivery_row in delivery_df.iterrows():
                shipment_date = delivery_row['Date de livraison souhaitee'].strftime('%Y%m%d') if pd.notna(delivery_row['Date de livraison souhaitee']) else ''
                e_line = [
                    f"E;{SALFCY};{SOHTYP};;{STOFCY};;;;{BPCORD};{ORDDAT};{CUSORDREF};{shipment_date};{adresse}"
                ]
                e_lines.extend(e_line)

    # Extract values for the L lines
    for _, product_row in products_df.iterrows():
        ITMREF = product_row['Code EAN']
        SAU = product_row['Type U.C.']
        QTY = product_row['Quant en UC']
        # Adjust the example line to match your required format
        l_line = [
            f"L;{ITMREF};{SAU};{QTY};;",  # Modify this example or add specific product descriptions if needed
          
        ]
        l_lines.extend(l_line)

    # Save to CSV files
    with open(os.path.join(output_dir, 'erp_sage.csv'), 'w') as file:
        for line in e_lines:
            file.write(f"{line}\n")
        for line in l_lines:
            file.write(f"{line}\n")



# Loop through the tables and concatenate those with matching headers
try:
    connection = pymysql.connect(**db_config)
    with connection.cursor() as cursor:
        entries = get_database_entries()

        for i, table in enumerate(tables):
            table_headers = [' '.join(col.split()).replace('\n', ' ').replace('\r', ' ').strip() for col in table.columns.tolist()]
            
            print(f"\nTable {i + 1} headers:")
            print(table_headers)

            for name, expected in normalized_expected_headers_with_names.items():
                if all(header in table_headers for header in expected):
                    if name == 'products_detail':
                        modify_code_ean_column(table, cursor)
                    
                    if name == 'orderer_details':
                        if 'Commande par' in table.columns:
                            for index, row in table.iterrows():
                                value = row['Commande par']
                                closest_match, uid, score = find_closest_match(value, entries)
                                if uid:
                                    table.at[index, 'Commande par'] = uid

                    table.columns = normalized_expected_headers_with_names[name]
                    concatenated_tables[name] = pd.concat([concatenated_tables[name], table], ignore_index=True)
                    print(f"Table {i + 1} concatenated to {name}")
                    break
            else:
                print(f"Table {i + 1} does not match expected headers.")
finally:
    connection.close()

# Save concatenated tables to CSV
for name, df in concatenated_tables.items():
    if not df.empty:
        csv_file_path = os.path.join(output_dir, f'{name}.csv')
        df.to_csv(csv_file_path, index=False)
        print(f"\n{name} table saved to {csv_file_path}")
        print(df)

# Generate ERP Sage CSV
if not (concatenated_tables['orderer_details'].empty or
        concatenated_tables['order_information'].empty or
        concatenated_tables['delivery_details'].empty or
        concatenated_tables['products_detail'].empty):
    generate_erp_sage_csv(
        concatenated_tables['orderer_details'],
        concatenated_tables['order_information'],
        concatenated_tables['delivery_details'],
        concatenated_tables['products_detail']
    )
    print("\nERP Sage CSV generated and saved.")
