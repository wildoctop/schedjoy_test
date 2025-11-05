

import psycopg2
import csv
import sys
import os
from datetime import datetime
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
WEBSITE = os.getenv("WEBSITE")

DB_CONFIG = {
    "host": DB_HOST,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "port": DB_PORT
}

# Define table names
PRODUCT_TABLE = "Product_DB"
VARIANT_TABLE = "Variant_DB"

# Output folder setup
OUTPUT_DIR = "../data"
ARCHIVE_DIR = "../archive"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
if not os.path.exists(ARCHIVE_DIR):
    os.makedirs(ARCHIVE_DIR)

# --- 1. Main Export and Transformation Function ---

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
                price, cost, compare, upc, weight, weight_grams, published, status, 
                debug_1, debug_2, debug_3
            FROM {} 
            WHERE status IN {}
            ORDER BY product_id, sku;
        """).format(
            sql.Identifier(VARIANT_TABLE),
            sql.Literal(TARGET_STATUSES) 
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
                inventory_quantity, debug_1, debug_2, debug_3, handle
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
            
            if pid in variants_by_product:
                for variant_row in variants_by_product[pid]:
                    
                    # Merge product and variant data for the flat CSV row
                    # Variant data (especially SKU, price, image) overwrites product data where keys conflict
                    merged_row = {**parent_row, **variant_row}
                    status = merged_row['status']
                    
                    # 1. Separate for Output Files
                    if status in STATUS_MAP:
                        separated_data[status].append(merged_row)
                    else:
                        draft_data.append(merged_row)
                        
                    # 2. Prepare Archive Copy (using the raw tuple with column order preserved)
                    if status in ['UPD', 'NEW', 'EXIST']:
                         # Create a tuple from the merged_row based on ALL_COLUMNS order
                         archive_rows.append(tuple(merged_row.get(col, None) for col in ALL_COLUMNS))
        
        
        # --- STEP 4: Write Output CSVs and Archive ---
        
        # a. Write UPD and NEW files
        for status, filename in STATUS_MAP.items():
            if separated_data[status]:
                output_path = os.path.join(OUTPUT_DIR, filename)
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    # Use the combined header list
                    writer = csv.DictWriter(f, fieldnames=ALL_COLUMNS, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(separated_data[status])
                print(f"Exported {len(separated_data[status])} records to {output_path}")

        # b. Write TO_DRAFT file
        if draft_data:
            draft_path = os.path.join(OUTPUT_DIR, "to_draft.csv")
            with open(draft_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=ALL_COLUMNS, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(draft_data)
            print(f"Exported {len(draft_data)} records to {draft_path} (Draft/Other status)")
            
        # c. Write Archive Copy
        if archive_rows:
            with open(archive_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(ALL_COLUMNS) # Write the combined headers
                writer.writerows(archive_rows)
            print(f"Archived {len(archive_rows)} records to {archive_filename}")
        
        
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
            status = row_dict['status']
            skus_by_status[status].append(row_dict['sku'])

        
        # Iterate through transitions and apply updates
        for old_status, new_status in STATUS_TRANSITIONS.items():
            skus_to_update = skus_by_status.get(old_status)
            
            if skus_to_update:
                update_query = sql.SQL("""
                    UPDATE {}
                    SET status = {}
                    WHERE sku IN ({})
                    AND status = {}
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