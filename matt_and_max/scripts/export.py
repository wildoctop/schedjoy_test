import numpy as np
import pandas as pd
import psycopg2
import csv
import sys
import os
from datetime import datetime
from psycopg2 import sql
from dotenv import load_dotenv
from typing import List, Dict, Any

script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, '..', 'local.env') 

load_dotenv(dotenv_path=dotenv_path)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
WEBSITE = os.getenv("WEBSITE")
VENDOR = os.getenv("VENDOR")


DB_CONFIG = {
    "host": DB_HOST,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "port": DB_PORT
}

# Define table names
PRODUCT_TABLE = "product_db"
VARIANT_TABLE = "variant_db"

# Output folder setup
OUTPUT_DIR = "../data"
ARCHIVE_DIR = "../archive"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
if not os.path.exists(ARCHIVE_DIR):
    os.makedirs(ARCHIVE_DIR)

# --- 1. Main Export and Transformation Function ---


COLUMN_RENAMES = {
    'title': 'Title',
    'image_url': 'Image Src',
    'descr': 'Body (HTML)',
    'cert': 'Useful links (product.metafields.custom.useful_links)',
    'opt_1': 'Option1 name',
    'opt_2': 'Option2 name',
    'opt_3': 'Option3 name',
    'tags': 'Tags',
    'product_category': 'Product Category',
    'type': 'Type',
    'vendor': 'Vendor',
    'inventory_tracker': 'Inventory tracker',
    'inventory_quantity': 'Inventory quantity',
    'handle': 'Handle',
    'var_image_url': 'Variant Image',
    'sku': 'Variant SKU',
    'opt_1_val': 'Option1 value',
    'opt_2_val': 'Option2 value',
    'opt_3_val': 'Option3 value',
    'price': 'Variant Price',
    'cost': 'Cost per item',
    'compare': 'Variant Compare At Price',
    'upc': 'Variant Barcode',
    'weight': 'Variant Grams',
    'weight_grams': 'Variant Weight Unit',
    'published': 'Published',
    'status': 'Status'
}


# 2. Define the FINAL Column Order (Crucial for CSV structure)
# List the renamed keys in the exact order you want them in the final file.
FINAL_COLUMNS = [
    'Handle',
    'Title',
    'Variant SKU',
    'Body (HTML)',
    'Useful links (product.metafields.custom.useful_links)',
    'Option1 name',
    'Option2 name',
    'Option3 name',
    'Option1 value',
    'Option2 value',
    'Option3 value',
    'Variant Compare At Price',
    'Variant Price',
    'Cost per item',
    'Tags',
    'Product Category',
    'Type',
    'Vendor',
    'Image Src',
    'Variant Image',
    'Inventory tracker',
    'Inventory quantity',
    'Variant Barcode',
    'is_variant_parent',
    'is_single_variant',
    'Status',
    'Variant Grams',
    'Variant Weight Unit',
    'product_group_id',
    'debug_1',
    'debug_2',
    'debug_3',
    'cat_name',
    'status_int'
]

def create_url_handle(title, sku=None):
    if pd.isna(title) or title == '':
        return np.nan # Or some default handle if title is missing
    # Simple example: replace spaces with hyphens and lowercase
    handle_title = str(title).lower().replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')
    handle_sku = str(sku).lower().replace(' ', '_').replace(',', '') if pd.notna(sku) and sku != '' else ''
    if handle_sku:
        return f"{handle_title}_{handle_sku}"
    return handle_title


def process_and_save_data(data_list: List[Dict[str, Any]], filename: str, final_columns: List[str]):
    """
    Converts a list of dictionaries into a DataFrame, renames and 
    reorders columns, and saves it to a CSV file.
    """
    if not data_list:
        print(f"No data to save for {filename}")
        return
    
    # 1. Convert list of dictionaries to a DataFrame
    df = pd.DataFrame(data_list)
    #print(df)
    # 2. Rename the columns
    df = df.rename(columns=COLUMN_RENAMES)
    
    #print(df)
    # 3. Reorder the columns using reindex (This is the most important step for order)
    # This also discards any columns not listed in FINAL_COLUMNS
    df = df.reindex(columns=FINAL_COLUMNS)
    
    df['Variant Price'] = df['Variant Price'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
    df['Variant Compare At Price'] = df['Variant Compare At Price'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
    df['Cost per item'] = df['Cost per item'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
    

    
    columns_to_clean = ['Variant Price', 'Variant Compare At Price', 'Cost per item']
        
    df[columns_to_clean] = df[columns_to_clean].replace("nan", "").replace("None", "").replace("N/A", "")

    
    for i in range(len(df)):
            if pd.notnull(df.loc[i, 'Title']): 
                if i < len(df) - 1 and (pd.isna(df.loc[i + 1, 'Title']) or df.loc[i + 1, 'Title'] == ''):
                    df.loc[i, 'is_variant_parent'] = True

   
    
    parent_indices = df[df['is_variant_parent'] == True].index
    for idx in parent_indices:
        title = df.loc[idx, 'Title']
        first_variant_sku = None
        next_parent_idx = parent_indices[parent_indices > idx].min() if any(parent_indices > idx) else len(df)
        variant_indices = []
        handle = None
        for i in range(idx + 1, next_parent_idx):
            row = df.loc[i]
            if pd.isna(row['Title']) and pd.notna(row['Variant SKU']):
                    variant_indices.append(i)
            

        if variant_indices:
            first_variant_sku = df.loc[variant_indices[0], 'Variant SKU'] # get the first sku
            handle = create_url_handle(title, first_variant_sku)  # Generate handle
            df.loc[idx, 'Handle'] = handle  

            for variant_idx in variant_indices:
  
                df.loc[variant_idx, 'Variant Handle'] = handle
                df.loc[variant_idx, 'Handle'] = df.loc[variant_idx, 'Variant Handle']

        # Create variant image URL
       
                
        
        #  Create handles for products without variants
    is_variant_mask = pd.notnull(df['Variant SKU']) & (pd.isnull(df['Title']) | (df['Title'] == '')) 
    standalone_mask = ~(df['is_variant_parent'] | is_variant_mask)
    new_handles = pd.Series(np.nan, index=df.index, dtype='object')

    new_handles[standalone_mask] = df.loc[standalone_mask].apply(
        lambda row: create_url_handle(row['Title'], row['Variant SKU']), axis=1
        )

    df.loc[standalone_mask, 'Handle'] = new_handles[standalone_mask]
        
    
    expanded_rows = []
    for index, row in df.iterrows():
        urls_str = str(row['Image Src'])
        urls = [url.strip() for url in urls_str.split(',') if url.strip()]
        current_url_handle = row['Handle']

        if not urls:
            expanded_rows.append(row.to_dict())
            continue

        first_url_row = row.copy()
        first_url_row['Variant Image'] = urls[0]
        first_url_row['Image Src'] = ''
        expanded_rows.append(first_url_row.to_dict())

        for i in range(1, len(urls)):
            new_row_data = {col: None for col in df.columns}
            new_row_data['Image Src'] = urls[i]
            new_row_data['Handle'] = current_url_handle
            
            expanded_rows.append(new_row_data)
    df = pd.DataFrame(expanded_rows)
    df['Barcode'] = df['Variant Barcode'].astype(str).str.replace("UPC ", "", regex=False)
    df['Variant Image'] = df['Variant Image'].replace("nan", "").replace("None", "").replace("N/A", "")
    df.loc[
    df['Variant Handle'].notna() & (df['Variant Handle'] != ''),
    'Handle'
    ] = df['Variant Handle']
    # 4. Save to CSV
    df.to_csv(filename, index=False, header=True)
    print(f"Successfully saved {len(data_list)} rows to {filename}")



def export_and_manage_data():
    """Connects to DB, exports data to CSVs, archives, and updates statuses."""
    
    conn = None
    cursor = None
    exit_code = 0
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_filename = os.path.join(ARCHIVE_DIR, f"archive_copy_{timestamp}.csv")
    

    TARGET_STATUSES = ('UPD', 'NEW', 'EXIST', 'NOT_READY')

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False 
        cursor = conn.cursor()



        variant_select_query = sql.SQL("""
            SELECT 
                var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val, 
                price, cost, compare, upc, weight, weight_grams, published, status_int, 
                debug_1, debug_2, debug_3
            FROM {} 
            WHERE status_int IN {} AND vendor = VENDOR
            ORDER BY product_id, sku;
        """).format(
            sql.Identifier(VARIANT_TABLE),
            sql.Literal(TARGET_STATUSES),
            sql.Literal(VENDOR)
        )

        cursor.execute(variant_select_query)
        variant_rows_tuples = cursor.fetchall()
        
        if not variant_rows_tuples:
            print("No relevant variant data found for export. Exiting.")
            sys.exit(0)

        variant_cols = [desc[0] for desc in cursor.description]
        
        # --- Group variants by product_id and build the set of product IDs needed ---
        variants_by_product = {}
        product_ids_to_fetch = set()
        
        for row_tuple in variant_rows_tuples:
            row_dict = dict(zip(variant_cols, row_tuple))
            pid = row_dict['product_id']
            product_ids_to_fetch.add(pid)
            if pid not in variants_by_product:
                variants_by_product[pid] = []
            variants_by_product[pid].append(row_dict)


        # --- STEP 2: Fetch corresponding Product data ---
        product_select_query = sql.SQL("""
            SELECT 
                product_id, cat, url, cat_name, title, sku, image_url, descr, cert, opt_1, opt_2, 
                opt_3, tags, product_category, type, vendor, inventory_tracker, 
                inventory_quantity, debug_1, debug_2, debug_3, handle, status
            FROM {} 
            WHERE product_id IN ({})
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            sql.SQL(', ').join(map(sql.Literal, list(product_ids_to_fetch)))
        )
        
        cursor.execute(product_select_query)
        product_rows_tuples = cursor.fetchall()
        product_cols = [desc[0] for desc in cursor.description]
        product_data_map = {row[0]: dict(zip(product_cols, row)) for row in product_rows_tuples}

        
        # --- STEP 3: Restructure Data for Hierarchical Export ---
        
        STATUS_MAP = {
            "UPD": f"{WEBSITE}_upd_for_shopify.csv",
            "NEW": f"{WEBSITE}_new_for_shopify.csv",
            # All other statuses will be grouped into 'to_draft'
        }
        
        separated_data = {key: [] for key in STATUS_MAP.keys()}
        draft_data = []
        archive_rows = []
        
        # Define the complete, unified header row for the CSV files
        # Includes all unique columns from both product and variant tables
        ALL_COLUMNS = list(set(product_cols) | set(variant_cols))
        
        # Iterate over products (to ensure parent row comes first)
        for pid in product_ids_to_fetch:
    
            if pid not in product_data_map:
                continue # Skip if product data wasn't fetched (shouldn't happen)
                
            # --- Get Parent Data ---
            parent_row = product_data_map[pid]
            parent_written = False 
            
            # 1. NEW LOGIC: DETERMINE PARENT STATUS FROM VARIANTS
            parent_status = 'EXIST' # Set a safe default status (e.g., 'EXIST' for products already known)
            
            if pid in variants_by_product:
                variants = variants_by_product[pid]

                # --- NEW RULE: ALL VARIANTS ARE NEW ---
                # Check if the list of variants is non-empty AND 
                # all variants have the status 'NEW'
                if variants and all(v.get('status_int') == 'NEW' for v in variants):
                    parent_status = 'NEW'
                    
                # --- PREVIOUS RULE: ANY VARIANT IS UPDATED ---
                # Only check for UPD if the status wasn't already determined as NEW
                elif any(v.get('status_int') == 'UPD' for v in variants):
                    parent_status = 'UPD'
                
                # Now, proceed to iterate through the variants
                for variant_row in variants:
                    variant_row['var_img'] = parent_row['image_url']
                    # --- 1. PROCESS AND SAVE PARENT ROW (Executed only once per PID) ---
                    if not parent_written:
                        
                        # A. Separate for Output Files
                        if parent_status in STATUS_MAP:
                            # Save the raw product data
                            parent_row['status_int'] = parent_status
                            separated_data[parent_status].append(parent_row)
                            parent_row['sku'] = ''
                        else:
                            parent_row['status_int'] = parent_status
                            parent_row['sku'] = ''
                            draft_data.append(parent_row)
                            
                        # B. Prepare Archive Copy 
                        if parent_status in ['UPD', 'NEW', 'EXIST']:
                            archive_rows.append(tuple(parent_row.get(col, None) for col in ALL_COLUMNS))
                        
                        parent_written = True # Mark parent as saved

                    # --- 2. PROCESS AND SAVE VARIANT ROW (Executed for every variant) ---
                    variant_status = variant_row['status_int'] # This is still correct for variants
                    
                    # A. Separate for Output Files
                    if variant_status in STATUS_MAP:
                        # Save the raw variant data
                        separated_data[variant_status].append(variant_row)
                    else:
                        draft_data.append(variant_row)
                        
                    # B. Prepare Archive Copy 
                    if variant_status in ['UPD', 'NEW', 'EXIST']:
                        archive_rows.append(tuple(variant_row.get(col, None) for col in ALL_COLUMNS))

        
        
        # --- STEP 4: Write Output CSVs and Archive ---
        
        # a. Write UPD and NEW files
        print("\n--- Exporting CSV Files ---")
        for status, filename in STATUS_MAP.items():
            filename = os.path.join(OUTPUT_DIR, filename)
            process_and_save_data(separated_data[status], filename, FINAL_COLUMNS)

        # b. Write TO_DRAFT file
        d_filename = os.path.join(OUTPUT_DIR, "to_draft.csv")
        process_and_save_data(draft_data, d_filename, FINAL_COLUMNS)
            
        # c. Write Archive Copy
        # NOTE: Archive is saved using ALL_COLUMNS list and tuples, not dicts, so we use it raw.
        # We pass ALL_COLUMNS as the final argument for the header/reindex in the helper.
        a_filename = os.path.basename(archive_filename)
        a_filename = os.path.join(ARCHIVE_DIR, a_filename) 
        process_and_save_data(archive_rows, a_filename, ALL_COLUMNS)

        
        
        # --- STEP 5: Update Statuses in Database (Using variant_rows_tuples for consistency) ---
        print("\n--- Updating Statuses in DB ---")
        
        STATUS_TRANSITIONS = {
            'EXIST': 'NOT_READY', # Change EXIST to NOT_READY
            'UPD':   'EXIST',     # Change UPD to EXIST
            'NEW':   'EXIST'      # Change NEW to EXIST
        }
        
        # Collect all SKUs that were exported (keyed by current status)
        skus_by_status = {status: [] for status in TARGET_STATUSES}
        for row_tuple in variant_rows_tuples:
            row_dict = dict(zip(variant_cols, row_tuple))
            status = row_dict['status_int']
            skus_by_status[status].append(row_dict['sku'])

        
        # Iterate through transitions and apply updates
        for old_status, new_status in STATUS_TRANSITIONS.items():
            skus_to_update = skus_by_status.get(old_status)
            
            if skus_to_update:
                update_query = sql.SQL("""
                    UPDATE {}
                    SET status_int = {}
                    WHERE sku IN ({})
                    AND status_int  = {}
                """).format(
                    sql.Identifier(VARIANT_TABLE),
                    sql.Literal(new_status),
                    sql.SQL(', ').join(map(sql.Literal, skus_to_update)),
                    sql.Literal(old_status)
                )
                
                cursor.execute(update_query)
                print(f"Updated {cursor.rowcount} variants from '{old_status}' to '{new_status}'.")
        
        
        # --- STEP 6. Commit and Cleanup ---
        conn.commit()
        print("\nDB transaction committed successfully.")
        
    except psycopg2.Error as e:
        print(f"\nFATAL: Database error occurred: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
            print("DB transaction rolled back.")
        exit_code = 2 
        
    except Exception as e:
        print(f"\nFATAL: An unexpected error occurred: {e}", file=sys.stderr)
        exit_code = 1 
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
        sys.exit(exit_code)


if __name__ == '__main__':
    export_and_manage_data()
