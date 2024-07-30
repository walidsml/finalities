import tabula
import camelot
import os
import pymysql.cursors
import pandas as pd
from fuzzywuzzy import process
import re
from datetime import datetime

# Database connection settings
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'SomathesProducts',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def extract_first_table_from_pdf(pdf_path, output_csv_path):
    tables = tabula.read_pdf(pdf_path, pages='1', multiple_tables=True)
    if tables:
        first_table = tables[0]
        print("First table extracted:")
        print(first_table)
        first_table.to_csv(output_csv_path, index=False)
        print(f"First table extracted and saved to '{output_csv_path}'")
    else:
        print("No tables found in the PDF.")

def extract_and_save_remaining_tables_from_pdf(pdf_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    tables = camelot.read_pdf(pdf_path, pages='all')
    for i, table in enumerate(tables):
        if i == 0:
            csv_path = os.path.join(output_dir, "delivery-details.csv")
        elif i == 1:
            csv_path = os.path.join(output_dir, "products_details.csv")
        elif i == 2:
            csv_path = os.path.join(output_dir, "others.csv")
        else:
            csv_path = os.path.join(output_dir, f"table_{i}.csv")
        table.to_csv(csv_path, index=False)
        print(f"Table {i} saved to {csv_path}")
        modify_table_with_database_data(csv_path, i)

def modify_table_with_database_data(csv_path, table_index):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            table_df = pd.read_csv(csv_path)
            if table_index == 0:
                modify_commande_par_column(table_df, cursor)
            else:
                modify_article_column(table_df, cursor)
            table_df.to_csv(csv_path, index=False)
            print(f"Modified CSV '{csv_path}' with database data successfully.")
    finally:
        connection.close()

def modify_commande_par_column(table_df, cursor):
    if 'Commande par' not in table_df.columns:
        raise ValueError("Column 'Commande par' not found in DataFrame.")
    for index, row in table_df.iterrows():
        value = row['Commande par']
        closest_match, uid, score = find_closest_match(value, cursor, 'clients', 'Den')
        if closest_match:
            table_df.at[index, 'Commande par'] = uid

def modify_article_column(table_df, cursor):
    if 'Article' not in table_df.columns:
        raise ValueError("Column 'Article' not found in DataFrame.")
    for index, row in table_df.iterrows():
        value = row['Article']
        sql = "SELECT UID FROM products WHERE EAN = %s"
        cursor.execute(sql, (value,))
        result = cursor.fetchone()
        if result:
            uid_value = result['UID']
            table_df.at[index, 'Article'] = uid_value

def get_database_entries(table_name):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            if table_name == 'products':
                sql = "SELECT EAN, UID FROM products"
            elif table_name == 'clients':
                sql = "SELECT Den, UID FROM clients"
            else:
                raise ValueError(f"Unknown table name '{table_name}'")
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()

def find_closest_match(input_text, cursor, table_name, lookup_column):
    entries = get_database_entries(table_name)
    input_store, input_city = split_store_city(input_text)
    best_match = None
    highest_score = 0
    for entry in entries:
        db_store, db_city = split_store_city(entry[lookup_column])
        store_score = process.extractOne(input_store, [db_store])[1]
        city_score = process.extractOne(input_city, [db_city])[1]
        score = (store_score + city_score) / 2
        if score > highest_score:
            highest_score = score
            best_match = entry
    if best_match:
        return best_match[lookup_column], best_match['UID'], highest_score
    else:
        return None, None, 0

def split_store_city(text):
    parts = re.split(r'\s+', text, maxsplit=1)
    store = parts[0]
    city = parts[1] if len(parts) > 1 else ''
    return store, city

def find_table_with_headers(pdf_path, headers):
    tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)
    for table_index, table in enumerate(tables):
        if all(header in table.columns for header in headers):
            print(f"Table with the required headers found (Table Index: {table_index}):")
            print(table.head())  # Print the first few rows for verification
            return table
    print("No table with the required headers found.")
    return None

def extract_and_format_date(table):
    if table is not None and 'Date de livraison souhaitee' in table.columns:
        raw_dates = table['Date de livraison souhaitee'].dropna()
        for raw_date in raw_dates:
            try:
                # Assuming the date format is 'dd/mm/yy hh:mm'
                date_obj = datetime.strptime(raw_date.split()[0], '%d/%m/%y')
                formatted_date = date_obj.strftime('%Y%m%d')
                print(f"Raw delivery date: {raw_date} -> Formatted delivery date: {formatted_date}")
            except ValueError as e:
                print(f"Error formatting date '{raw_date}': {e}")

def generate_sage_erp_csv(delivery_details_path, order_details_path, products_details_path, output_csv_path):
    delivery_df = pd.read_csv(delivery_details_path)
    order_df = pd.read_csv(order_details_path)
    products_df = pd.read_csv(products_details_path)

    print("Delivery Details DataFrame:")
    print(delivery_df)
    
    print("Order Details DataFrame:")
    print(order_df)

    print("Products Details DataFrame:")
    print(products_df)

    BPCORD = delivery_df['Commande par'].iloc[0]
    CUSORDREF = order_df['No commande'].iloc[0]

    if pd.isna(CUSORDREF):
        CUSORDREF = order_df['No commande'].iloc[1]

    ORDDAT = datetime.today().strftime('%Y%m%d')

    # Extract and format the Date de livraison souhaitee
    delivery_date = None
    if 'Date de livraison souhaitee' in delivery_df.columns:
        raw_date = delivery_df['Date de livraison souhaitee'].iloc[0]
        print("Raw delivery date:", raw_date)
        try:
            delivery_date = datetime.strptime(raw_date.split()[0], '%d/%m/%y').strftime('%Y%m%d')
            print("Formatted delivery date:", delivery_date)
        except ValueError:
            print("Error in date format for 'Date de livraison souhaitee'")

    sage_erp_lines = []
    if delivery_date:
        sage_erp_lines.append(f"E;AL1;SON;;{BPCORD};{ORDDAT};;{CUSORDREF};AL1;{delivery_date};ADR;;;;")
    else:
        sage_erp_lines.append(f"E;AL1;SON;;{BPCORD};{ORDDAT};;{CUSORDREF};AL1;;ADR;;;;")

    for _, row in products_df.iterrows():
        ITMREF = row['Article']
        QTY = None
        if 'Quant en\nUC' in products_df.columns:
            QTY = row['Quant en\nUC']
        elif 'UVC/UC Quant en' in products_df.columns:
            QTY = row['UVC/UC Quant en']
        
        if QTY is not None:
            sage_erp_lines.append(f"L;{ITMREF};;;{QTY};;;;;")

    with open(output_csv_path, 'w') as file:
        file.write('\n'.join(sage_erp_lines))

    print(f"Sage ERP CSV saved to {output_csv_path}")

if __name__ == "__main__":
    pdf_path = "templatefull1.pdf"
    first_table_csv_path = "/Users/walid/Desktop/result/temp2/order-details.csv"
    remaining_tables_output_dir = "/Users/walid/Desktop/result/temp2/remaining_tables/"
    sage_erp_csv_path = "/Users/walid/Desktop/result/temp2/sage_erp.csv"

    extract_first_table_from_pdf(pdf_path, first_table_csv_path)
    extract_and_save_remaining_tables_from_pdf(pdf_path, remaining_tables_output_dir)
    
    delivery_details_path = os.path.join(remaining_tables_output_dir, "delivery-details.csv")
    products_details_path = os.path.join(remaining_tables_output_dir, "products_details.csv")

    # Find and process the table with specific headers
    table_with_dates = find_table_with_headers(pdf_path, ['Date de livraison souhaitee', 'Date de livraison limite'])
    extract_and_format_date(table_with_dates)
    
    generate_sage_erp_csv(delivery_details_path, first_table_csv_path, products_details_path, sage_erp_csv_path)
