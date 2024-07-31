import pdfplumber
import pandas as pd
import pymysql
import os
from fuzzywuzzy import process
import re
from datetime import datetime

# Path to the PDF file
pdf_path = 'marjanevrac.pdf'
# Directory to save CSV files
output_dir = '/Users/walid/Desktop/result/temp2'
erp_sage_csv_path = os.path.join(output_dir, 'erp_sage.csv')

# Define header-to-name mapping
header_to_name = {
    tuple(['Nocommande', 'Datecommande', 'Codefournisseur', 'Contratcommercial', 'Filiere', 'Etatcommande']): 'order details',
    tuple(['Commandepar', 'Livrea', 'Commandea']): 'ordered details',
    tuple(['Article', 'Libellearticle', 'VL', 'Noligne', 'TypeU.C.', 'Quanten\nUC', 'UVC/UC', 'Quanten\nUVC', 'No.operation\nspeciale']): 'products details',
    tuple(['Datedelivraisonsouhaitee', 'Datedelivraisonlimite']): 'delivery details'
}

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'SomathesProducts',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_database_entries():
    # Connect to the database
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            # Select the relevant columns from the table
            sql = "SELECT UID, Den FROM clients"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()

def find_closest_match(input_text, entries):
    # Split the input text into store name and city name
    input_store, input_city = split_store_city(input_text)
    
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
    # Example splitting method, customize as per your database format
    parts = re.split(r'\s+', text, maxsplit=1)
    store = parts[0]
    city = parts[1] if len(parts) > 1 else ''
    return store, city

def modify_article_column(table_df, cursor):
    # Ensure the specified column exists in the DataFrame
    if 'Article' not in table_df.columns:
        raise ValueError("Column 'Article' not found in DataFrame.")

    # Iterate through each row and update the 'Article' column with 'UID'
    for index, row in table_df.iterrows():
        value = row['Article']

        # Find UID for the given Article in the products table
        sql = "SELECT UID FROM products WHERE EAN = %s"
        cursor.execute(sql, (value,))
        result = cursor.fetchone()

        if result:
            uid_value = result['UID']
            table_df.at[index, 'Article'] = uid_value

def reformat_date(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%y%H:%M").strftime("%Y%m%d")
    except ValueError:
        return None

def generate_erp_sage_csv(ordered_details, order_details, delivery_details, products_details):
    if ordered_details is None:
        print("ordered_details table is missing.")
    if order_details is None:
        print("order_details table is missing.")
    if delivery_details is None:
        print("delivery_details table is missing.")
    if products_details is None:
        print("products_details table is missing.")
    
    if ordered_details is None or order_details is None or delivery_details is None or products_details is None:
        print("One or more required tables are missing.")
        return
    
    with open(erp_sage_csv_path, 'w') as file:
        # Write E line
        BPCORD = ordered_details['Commandepar'].iloc[0] if 'Commandepar' in ordered_details.columns else ''
        ORDDAT = reformat_date(order_details['Datecommande'].iloc[0]) if 'Datecommande' in order_details.columns else ''
        CUSORDREF = order_details['Nocommande'].iloc[0] if 'Nocommande' in order_details.columns else ''
        shipment_date = reformat_date(delivery_details['Datedelivraisonsouhaitee'].iloc[0]) if 'Datedelivraisonsouhaitee' in delivery_details.columns else ''
        E_line = f"E;AL1;SON;;{BPCORD};{ORDDAT};{CUSORDREF};AL1;{shipment_date};ADR\n"
        file.write(E_line)
        
        # Write L lines
        for _, row in products_details.iterrows():
            ITMREF = row['Article']
            QTY = row['Quanten\nUC']
            L_line = f"L;{ITMREF};CAR;{QTY}\n"
            file.write(L_line)

def delete_csv_files(directory, except_file):
    for file in os.listdir(directory):
        if file.endswith(".csv") and file != os.path.basename(except_file):
            os.remove(os.path.join(directory, file))

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Initialize a dictionary to store concatenated tables
concatenated_tables = {}
printed_tables = set()  # To keep track of headers already printed

# Open the PDF file
with pdfplumber.open(pdf_path) as pdf:
    # Connect to the database
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        # Get the database entries for fuzzy matching
        entries = get_database_entries()

        # Iterate over all the pages in the PDF
        for page in pdf.pages:
            # Extract tables from the page
            tables = page.extract_tables()
            
            # Process each table
            for table in tables:
                if table:  # Check if the table is not empty
                    headers = tuple(table[0])  # Convert headers to tuple for easy lookup
                    table_name = header_to_name.get(headers, None)

                    if table_name:
                        if table_name == 'products details':
                            # Handle concatenation for 'products details'
                            if table_name not in concatenated_tables:
                                # First occurrence, add with headers
                                concatenated_tables[table_name] = pd.DataFrame(table[1:], columns=table[0])
                            else:
                                # Subsequent occurrences, add data without headers
                                df = pd.DataFrame(table[1:], columns=table[0])
                                concatenated_tables[table_name] = pd.concat([concatenated_tables[table_name], df], ignore_index=True)
                        elif table_name == 'ordered details':
                            # Combine 'ordered details' tables into one DataFrame
                            if 'ordered details' not in concatenated_tables:
                                concatenated_tables['ordered details'] = pd.DataFrame(table[1:], columns=table[0])
                            else:
                                df = pd.DataFrame(table[1:], columns=table[0])
                                concatenated_tables['ordered details'] = pd.concat([concatenated_tables['ordered details'], df], ignore_index=True)
                        else:
                            # Print the type of table
                            print(f"Table type: {table_name}")

                            # Save the table as a CSV file
                            df = pd.DataFrame(table[1:], columns=table[0])  # Convert table data to DataFrame
                            csv_path = os.path.join(output_dir, f"{table_name}.csv")
                            df.to_csv(csv_path, index=False)  # Save DataFrame to CSV
                            print(f"Saved {csv_path}")

        # Modify 'Article' column in 'products details' tables
        if 'products details' in concatenated_tables:
            df_products = concatenated_tables['products details']
            modify_article_column(df_products, cursor)

            # Save the modified 'products details' table
            csv_path = os.path.join(output_dir, 'products_details_combined.csv')
            df_products.to_csv(csv_path, index=False)  # Save combined DataFrame to CSV
            print(f"Saved {csv_path}")

        # Replace 'Commandepar' column in 'ordered details' with closest match UID
        if 'ordered details' in concatenated_tables:
            df_ordered = concatenated_tables['ordered details']
            if 'Commandepar' in df_ordered.columns:
                df_ordered['Commandepar'] = df_ordered['Commandepar'].apply(
                    lambda x: find_closest_match(x, entries)[1] if pd.notnull(x) else x
                )

            # Save the updated 'ordered details' table
            csv_path = os.path.join(output_dir, 'ordered_details_combined.csv')
            df_ordered.to_csv(csv_path, index=False)  # Save updated DataFrame to CSV
            print(f"Saved updated {csv_path}")

        # Generate ERP Sage CSV
        generate_erp_sage_csv(
            concatenated_tables.get('ordered details', None),
            pd.read_csv(os.path.join(output_dir, 'order details.csv')) if os.path.exists(os.path.join(output_dir, 'order details.csv')) else None,
            pd.read_csv(os.path.join(output_dir, 'delivery details.csv')) if os.path.exists(os.path.join(output_dir, 'delivery details.csv')) else None,
            concatenated_tables.get('products details', None)
        )

        # Delete other CSV files except ERP Sage CSV
        delete_csv_files(output_dir, erp_sage_csv_path)

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
