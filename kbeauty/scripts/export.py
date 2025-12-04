import re
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
VARIANT_COLS_TO_MERGE = [
    'Variant SKU', 
    'Variant Compare At Price', 
    'Variant Price', 
    'Variant Barcode'
]

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

def replace_cat_optimized(df, old_cat, new_cat, replacement_map=None):
    df[new_cat] = ''
    
    # 1. Create a mapping dictionary for easier lookup, ensuring keys are stripped of whitespace
    # This also handles escaping special regex characters
    rev_map = {re.escape(k.strip()): v for k, v in replacement_map.items()}
    
    # 2. Build the pattern by joining all keys
    pattern = '|'.join(rev_map.keys())

    # 3. Use str.findall to get all matches for each cell.
    # The findall will return the exact substring that was matched from the original string.
    # Add a word boundary or a simple lookaround to ensure you're matching a full token, not a substring
    matches = df[old_cat].astype(str).str.findall(f'({pattern})')
    
    # 4. Map the found matches to their new categories
    df[new_cat] = matches.apply(
        lambda x: ', '.join(sorted(list(set(rev_map[re.escape(m.strip())] for m in x)))) if x else ''
    )
    return df

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
    
    type_replacement_mapping = {
           "Facial Cleansers" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Facial Cleansers",
           "Toners" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Toners & Astringents",
           "Exfoliators" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Skin Care Masks & Peels",
           "Sheet Masks" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Skin Care Masks & Peels",
           "Serums" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Face Serums",
           "Emulsions & Essences" : "Health & Beauty > Personal Care > Cosmetics > Skin Care",
           "Moisturizers" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Face Moisturizers",
           "Gel Moisturizers" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Face Moisturizers",
           "Eye Care / Eye and Lips" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Eye Creams",
           "Lip Balm" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Lip Balms & Treatments > Lip Balms",
           "Makeup Removers" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Makeup Removers",
           "Skincare Kits" : "Health & Beauty > Personal Care > Cosmetics > Cosmetic Tools > Skin Care Tools",
           "Perfume" : "Health & Beauty > Personal Care > Cosmetics > Perfumes & Colognes",
           "Bath & Shower" : "Health & Beauty > Personal Care > Cosmetics > Bath & Body",
           "Hand & Foot Cream" : "Health & Beauty > Personal Care > Cosmetics > Skin Care > Hand Creams",
           "Hair Color" : "Health & Beauty > Personal Care > Hair Care > Hair Color",
           "Hair Styling" : "Health & Beauty > Personal Care > Hair Care > Hair Styling Products",
           "MakeUp": "Health & Beauty > Personal Care > Cosmetics > Makeup"

        }
    df = replace_cat_optimized(
            df,
            old_cat='cat_name',
            new_cat='Product category',
            replacement_map=type_replacement_mapping
        ) 

    type_replacement_mapping = {
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Facial Cleansers": "Facial Cleansers",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Toners & Astringents": "Toners & Astringents",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Skin Care Masks & Peels": "Skin Care Masks & Peels",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Skin Care Masks & Peels": "Skin Care Masks & Peels",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Face Serums": "Face Serums",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care": "Skin Care",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Face Moisturizers": "Face Moisturizers",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Face Moisturizers": "Face Moisturizers",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Eye Creams": "Eye Creams",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Lip Balms & Treatments > Lip Balms": "Lip Balms",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Makeup Removers": "Makeup Removers",
           "Health & Beauty > Personal Care > Cosmetics > Cosmetic Tools > Skin Care Tools": "Skin Care Tools",
           "Health & Beauty > Personal Care > Cosmetics > Perfumes & Colognes": "Perfumes & Colognes",
           "Health & Beauty > Personal Care > Cosmetics > Bath & Body": "Bath & Body",
           "Health & Beauty > Personal Care > Cosmetics > Skin Care > Hand Creams": "Hand Creams",
           "Health & Beauty > Personal Care > Hair Care > Hair Color": "Hair Color",
           "Health & Beauty > Personal Care > Hair Care > Hair Styling Products": "Hair Styling Products",
           "Health & Beauty > Personal Care > Cosmetics > Makeup": "Makeup"
           }
        
    df = replace_cat_optimized(
            df,
            old_cat='Product category',
            new_cat='Type',
            replacement_map=type_replacement_mapping
        ) 
    
    df['Tags'] = df['cat_name']
    
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
    
    processed_rows = []
    
    # 1. Group by Handle
    grouped = df.groupby('Handle')
    
    for handle, group in grouped:
        # Identify the main product row (Title is not null)
        product_rows = group[group['Title'].notna()]
        
        # Identify valid variant rows (SKU is not null)
        variant_rows = group[group['Variant SKU'].notna()]
        
        # Identify the single main product row (should only be one)
        # We take the first one found, if any.
        main_product_row_idx = product_rows.index[0] if not product_rows.empty else None
        
        # --- LOGIC FOR SINGLE VARIANT MERGE ---
        
        # Check for exactly one variant AND a corresponding product row
        if len(variant_rows) == 1 and main_product_row_idx is not None:
            # Found a product with exactly one variant: MERGE
            
            variant_row_idx = variant_rows.index[0]
            
            # Get data for merging
            variant_data = df.loc[variant_row_idx, VARIANT_COLS_TO_MERGE].to_dict()
            
            # Get the product row dictionary for modification
            product_row_dict = df.loc[main_product_row_idx].to_dict()
            
            # Copy variant data to the product row
            for col, value in variant_data.items():
                product_row_dict[col] = value
                
            # 1. Append the modified (merged) product row
            processed_rows.append(product_row_dict)
            
            # 2. Append all auxiliary/image rows ("other rows")
            
            # Identify the indices of the product and variant row that are now replaced
            indices_to_skip = {main_product_row_idx, variant_row_idx}
            
            # Iterate through all original rows in the group
            for idx in group.index:
                if idx not in indices_to_skip:
                    # This row is neither the product nor the single variant. 
                    # It must be an image row or other auxiliary data. Append it "as is."
                    processed_rows.append(df.loc[idx].to_dict())
                    
        else:
            # --- LOGIC FOR MULTIPLE VARIANTS, ZERO VARIANTS, OR MISSING PRODUCT ROW ---
            # Append all original rows in the group unchanged (as requested).
            
            # Append all rows from the current group, maintaining original order
            for idx in group.index:
                processed_rows.append(df.loc[idx].to_dict())


    # Create the final DataFrame from the processed rows
    df = pd.DataFrame(processed_rows)
    df = df.reset_index(drop=True)

    columns_to_clean = ['Variant Price', 'Variant Compare At Price', 'Cost per item', 'Type', 'Tags', 'Product category', 'Variant Image']
        
    df[columns_to_clean] = df[columns_to_clean].replace("nan", "").replace("None", "")
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
            WHERE status_int IN {} AND vendor = {}
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
        ''' for old_status, new_status in STATUS_TRANSITIONS.items():
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
        '''
        
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
