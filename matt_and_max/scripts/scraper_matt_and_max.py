import re
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from pandas.core.methods.describe import describe_numeric_1d
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException
import csv
import os
import itertools
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql, extras
import sys
from typing import Dict, Any, Tuple, Optional, List, Union, Set, Any

script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, '..', 'local.env') 

load_dotenv(dotenv_path=dotenv_path)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

DB_CONFIG = {
    "host": DB_HOST,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "port": DB_PORT
}


# TRUE - IF URL LIST .CSV FILE IS READY
CSV_READY = False
CSV = '../data/matt_and_max_url.csv'
PROD_DEBUG_FILE = '../data/debug_matt_and_max.log'
URL_DEBUG_FILE = '../data/debug_matt_and_max_url.log'


URL_1 = "https://www.matandmax.com/ca-en/products/tools/brushes"  # Product list page
URL_2 = "https://www.matandmax.com/ca-en/products/tools/combs"  # Product list page
URL_3 = "https://www.matandmax.com/ca-en/products/tools/hair-clips-elastics-headbands"  # Product list page
URL_4 = "https://www.matandmax.com/ca-en/products/tools/hair-dryers"  # Product list page
URL_5 = "https://www.matandmax.com/ca-en/products/tools/hot-rollers-and-hair-setters"  # Product list page
URL_6 = "https://www.matandmax.com/ca-en/products/tools/scissors-blades"  # Product list page
URL_7 = "https://www.matandmax.com/ca-en/products/tools/clippers-razors"  # Product list page
URL_8 = "https://www.matandmax.com/ca-en/products/tools/curling-irons"  # Product list page
URL_9 = "https://www.matandmax.com/ca-en/products/tools/hair-straighteners"  # Product list page
URL_10 = "https://www.matandmax.com/ca-en/products/tools/maintenance"  # Product list page
URL_11 = "https://www.matandmax.com/ca-en/products/tools/personal-protective-gear"  # Product list page
URL_12 = "https://www.matandmax.com/ca-en/products/tools/texturizing-irons"  # Product list page
URL_13 = "https://www.matandmax.com/ca-en/products/tools/sports"  # Product list page
URL_14 = "https://www.matandmax.com/ca-en/products/nails/acrylics-resins"  # Product list page
URL_15 = "https://www.matandmax.com/ca-en/products/nails/builder-gel"  # Product list page
URL_16 = "https://www.matandmax.com/ca-en/products/nails/gel-polish"  # Product list page
URL_17 = "https://www.matandmax.com/ca-en/products/nails/nail-polish"  # Product list page
URL_18 = "https://www.matandmax.com/ca-en/products/nails/tools-and-accessories"  # Product list page
URL_19 = "https://www.matandmax.com/ca-en/products/nails/treatments"  # Product list page
URL_20 = "https://www.matandmax.com/ca-en/products/nails/lacquer-remover"  # Product list page
URL_21 = "https://www.matandmax.com/ca-en/products/hair-care/beard-products"  # Product list page
URL_22 = "https://www.matandmax.com/ca-en/products/hair-care/extensions"  # Product list page
URL_23 = "https://www.matandmax.com/ca-en/products/hair-care/conditioners"  # Product list page
URL_24 = "https://www.matandmax.com/ca-en/products/hair-care/hair-loss"  # Product list page
URL_25 = "https://www.matandmax.com/ca-en/products/hair-care/hair-treatment"  # Product list page
URL_26 = "https://www.matandmax.com/ca-en/products/hair-care/pumps"  # Product list page
URL_27 = "https://www.matandmax.com/ca-en/products/hair-care/kids"  # Product list page
URL_28 = "https://www.matandmax.com/ca-en/products/hair-care/shampoos"  # Product list page
URL_29 = "https://www.matandmax.com/ca-en/products/hair-care/speciality-products"  # Product list page
URL_30 = "https://www.matandmax.com/ca-en/products/hair-care/styling-products"  # Product list page
URL_31 = "https://www.matandmax.com/ca-en/products/hair-care/travel"  # Product list page
URL_32 = "https://www.matandmax.com/ca-en/products/beauty/accessories"  # Product list page
URL_33 = "https://www.matandmax.com/ca-en/products/beauty/bath"  # Product list page
URL_34 = "https://www.matandmax.com/ca-en/products/beauty/body"  # Product list page
URL_35 = "https://www.matandmax.com/ca-en/products/beauty/skin-care"  # Product list page
URL_36 = "https://www.matandmax.com/ca-en/products/beauty/makeup"  # Product list page
URL_37 = "https://www.matandmax.com/ca-en/products/color/care"  # Product list page
URL_38 = "https://www.matandmax.com/ca-en/products/color/color-accessories"  # Product list page
URL_39 = "https://www.matandmax.com/ca-en/products/color/demi-permanent-colors"  # Product list page
URL_40 = "https://www.matandmax.com/ca-en/products/color/developers-bleaches"  # Product list page
URL_41 = "https://www.matandmax.com/ca-en/products/color/express-permanent-colors"  # Product list page
URL_42 = "https://www.matandmax.com/ca-en/products/color/instant-touch-up"  # Product list page
URL_43 = "https://www.matandmax.com/ca-en/products/color/permanent-colors"  # Product list page
URL_44 = "https://www.matandmax.com/ca-en/products/color/perms"  # Product list page
URL_45 = "https://www.matandmax.com/ca-en/products/color/straighteners-and-relaxers"  # Product list page
URL_46 = "https://www.matandmax.com/ca-en/products/color/toners"  # Product list page
URL_47 = "https://www.matandmax.com/ca-en/products/sales/all"  # Product list page



# UPDATE WITH REAL DATA
ITEM = "li.product-impression.flex-none.border-l.border-gray-2.flex.flex-col.px-4.py-2.relative.mb-8.sm\\:mb-12.xl\\:mb-16"
NAME = "h1" # Product name CSS selector
SKU = ".code.variant-modelnumber"
DESC = "div.customer-service" # Product description CSS selector from product page
IMAGE = "div.flex.justify-center.items-center.w-full.h-full" # Product image url CSS selector
CERT_BLOCK = ".section.specs.primary-bg"


#



headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://google.com",
    "DNT": "0"
}

PRODUCT_TABLE = 'product_db'
VARIANT_LOOKUP_TABLE = 'variant_db'

# Note: I am assuming the connection and transaction (conn) is managed by the caller,
# and this function only executes queries and does NOT manage the connection itself.

def prepare_data_for_sql(value: Any) -> Optional[Union[str, int, float, bool]]:
    """
    Standardizes Python values for safe insertion into PostgreSQL via psycopg2.
    1. Converts sets and lists (of primitives) to comma-separated strings.
    2. Converts empty strings ("") to None (SQL NULL).
    3. Returns None for None.
    """
    if value is None:
        return None
    
    # 1. Handle collections (sets and lists) by converting them to strings
    if isinstance(value, (set, list)):
        # Join list/set items into a comma-separated string
        try:
            # Use map(str, value) to ensure all items are strings before joining
            return ", ".join(map(str, value))
        except Exception as e:
            # Fallback if the collection contains complex, unjoinable objects
            print(f"Warning: Could not stringify collection {value}. Error: {e}. Sending NULL.")
            return None

    # 2. Handle empty strings (crucial for numeric fields like price)
    if isinstance(value, str) and value.strip() == "":
        return None # Translates to SQL NULL
    
    # 3. Handle other types (int, float, bool, non-empty str, etc.)
    return value

def upsert_single_variant(
    product_data: Dict[str, Any], 
    target_sku: str, 
    cursor
) -> Tuple[Optional[str], Optional[int]]:
    target_sku = str(target_sku)
    # A. LOOKUP by SKU
    select_query = sql.SQL("SELECT product_id FROM {} WHERE sku = {}").format(
        sql.Identifier(PRODUCT_TABLE),
        sql.Literal(target_sku)
    )
    cursor.execute(select_query)
    result = cursor.fetchone()
    product_id = result[0] if result else None
    
    db_status = None
    image_urls: List[str] = product_data.get('Image Src', [])
    image_urls = list(image_urls)
    if image_urls:
        product_data['var_img'] = product_data.get('Image Src', [])
    else:
        product_data['var_img'] = ""

    # --- B. UPDATE LOGIC (If product_id exists - Rule 2) ---
    if product_id:
        status_to_set = 'UPD'
        db_status = 'UPD'

        # 1. Product Table UPDATE
        update_product_query = sql.SQL("""
            UPDATE {}
            SET 
                cat = {}, url = {}, cat_name = {}, title = {}, sku = {}, image_url = {}, 
                descr = {}, cert = {}, opt_1 = {}, opt_2 = {}, opt_3 = {}, tags = {}, 
                product_category = {}, type = {}, vendor = {}, inventory_tracker = {}, 
                inventory_quantity = {}, debug_1 = {}, debug_2 = {}, debug_3 = {},
                handle = {}, status_int = {}, status = {} 
            WHERE product_id = {}
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            sql.Literal(prepare_data_for_sql(product_data.get("cat", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("url", None))), 
            sql.Literal(prepare_data_for_sql(product_data.get("cat_name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Title", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Variant SKU", None))), # Use Variant SKU for product.sku
            sql.Literal(prepare_data_for_sql(product_data.get("Image Src", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Body (HTML)", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cert", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option1 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option2 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option3 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("tags", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("product_category", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("type", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Vendor", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_tracker", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_quantity", None))), # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("debug_1", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_2", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_3", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Handle", None))), 
            sql.Literal(status_to_set),
            sql.Literal(prepare_data_for_sql(product_data.get("Status", None))),
            sql.Literal(product_id)
        )
        cursor.execute(update_product_query)



        # 2. Variant Table UPSERT (for the single variant, using target_sku as var_id)
        upsert_variant_query = sql.SQL("""
            INSERT INTO {} (
                var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val, 
                price, cost, compare, upc, weight, weight_grams, published, status_int, 
                debug_1, debug_2, debug_3, vendor
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            )
            ON CONFLICT (var_id) DO UPDATE SET 
                var_image_url = EXCLUDED.var_image_url,
                price = EXCLUDED.price, cost = EXCLUDED.cost, compare = EXCLUDED.compare,
                upc = EXCLUDED.upc, weight = EXCLUDED.weight, published = EXCLUDED.published,
                status = EXCLUDED.status;
        """).format(
            sql.Identifier(VARIANT_LOOKUP_TABLE),
            # Variant VALUES (using single product_data dict)
            sql.Literal(target_sku), 
            sql.Literal(product_id),
            sql.Literal(prepare_data_for_sql(product_data.get("Handle", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("var_img", None))),
            sql.Literal(prepare_data_for_sql(target_sku)),
            sql.Literal(prepare_data_for_sql(product_data.get("Option1 value", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option2 value", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option3 value", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Variant Price", None))),        # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("cost", None))),         # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("Variant Compare At Price", None))),      # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("Variant Barcode", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("weight", None))),       # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("weight_grams", None))), # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("published", None))),
            sql.Literal(status_to_set),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_1", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_2", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_3", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Vendor", None)))
        )
        cursor.execute(upsert_variant_query)

        
        
    # --- C. INSERT LOGIC (If product_id does NOT exist - Rule 1) ---
    else:
        status_to_set = 'NEW'
        db_status = 'NEW'
        
        # 1. Product Table INSERT (RETURNING product_id)
        insert_product_query = sql.SQL("""
            INSERT INTO {} (
                cat, url, cat_name, title, sku, image_url, descr, cert, opt_1, opt_2, opt_3, 
                tags, product_category, type, vendor, inventory_tracker, inventory_quantity, 
                debug_1, debug_2, debug_3, handle, status, status_int
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 
                {}, {}, {}, {}, {}, {}, 
                {}, {}, {}, {}, {}, {}
            )
            RETURNING product_id;
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            sql.Literal(prepare_data_for_sql(product_data.get("cat", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("url", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cat_name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Title", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Variant SKU", None))), # Use Variant SKU for product.sku
            sql.Literal(prepare_data_for_sql(product_data.get("Image Src", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Body (HTML)", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cert", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option1 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option2 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option3 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("tags", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("product_category", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("type", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Vendor", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_tracker", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_quantity", None))), # IMPORTANT: Use None for numeric
            sql.Literal(prepare_data_for_sql(product_data.get("debug_1", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_2", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_3", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Handle", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Status", None))),
            sql.Literal(status_to_set)
        )
        cursor.execute(insert_product_query)
        product_id = cursor.fetchone()[0] 
        
        # 2. Variant Table INSERT (for the single variant)
        insert_variant_query = sql.SQL("""
            INSERT INTO {} (
                var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val,
                price, cost, compare, upc, weight, weight_grams, published, status_int,
                debug_1, debug_2, debug_3, vendor
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            );
        """).format(
            sql.Identifier(VARIANT_LOOKUP_TABLE),
            sql.Literal(target_sku),
            sql.Literal(product_id),
            sql.Literal(prepare_data_for_sql(product_data.get("Handle", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("var_img", None))),
            sql.Literal(prepare_data_for_sql(target_sku)),
            sql.Literal(prepare_data_for_sql(product_data.get("Option1 value", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option2 value", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option3 value", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Variant Price", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cost", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Variant Compare At Price", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Variant Barcode", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("weight", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("weight_grams", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("published", None))),
            sql.Literal(status_to_set),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_1", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_2", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_3", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Vendor", None)))
        )
        cursor.execute(insert_variant_query)

        

    return db_status, product_id

# --- HELPER FUNCTION 2: Multi-Variant Logic (Rules 3 & 4) ---

def upsert_multi_variant(
    product_data: Dict[str, Any], 
    variants: List[Dict[str, Any]], 
    cursor
) -> Tuple[Optional[str], Optional[int]]:

    # A. LOOKUP by Handle
    variant_skus = [v.get("Variant SKU") for v in variants if v.get("Variant SKU")]

    product_id = None
    
    if variant_skus:
        # 2. Build the WHERE var_id IN (...) clause from the list of SKUs
        print(len(variant_skus))
        sku_literals = sql.SQL(', ').join(sql.Literal(sku) for sku in variant_skus)
        
        select_query = sql.SQL("""
            SELECT product_id 
            FROM {} 
            WHERE var_id IN ({})
            LIMIT 1
        """).format(
            sql.Identifier(VARIANT_LOOKUP_TABLE),
            sku_literals
        )
        
        cursor.execute(select_query)
        result = cursor.fetchone()
        product_id = result[0] if result else None
    
    db_status = None

    # --- UPD PATH (Rule 3: Product exists, Update product and variants) ---
    if product_id:
        status_to_set = 'UPD'
        db_status = 'UPD'

        # 1. Update Product Table
        update_product_query = sql.SQL("""
            UPDATE {} SET 
                cat = {}, url = {}, cat_name = {}, title = {}, image_url = {}, 
                descr = {}, cert = {}, opt_1 = {}, opt_2 = {}, opt_3 = {}, 
                tags = {}, product_category = {}, type = {}, vendor = {}, 
                inventory_tracker = {}, inventory_quantity = {}, debug_1 = {}, 
                debug_2 = {}, debug_3 = {}, status_int = {}, status = {}, handle = {}
            WHERE product_id = {}
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            sql.Literal(prepare_data_for_sql(product_data.get("cat", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("url", None))), 
            sql.Literal(prepare_data_for_sql(product_data.get("cat_name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Title", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Image Src", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Body (HTML)", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cert", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option1 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option2 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option3 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("tags", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("product_category", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("type", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Vendor", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_tracker", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_quantity", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_1", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_2", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_3", None))),
            sql.Literal(status_to_set),
            sql.Literal(prepare_data_for_sql(product_data.get("Status", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Handle", None))),
            sql.Literal(product_id)
            
        )
        cursor.execute(update_product_query)

        # 2. Loop and UPSERT Variants (Update existing, Insert new)
        for variant in variants:
            var_sku = variant.get("Variant SKU")
            if not var_sku:
                print(f"Warning: Variant  is missing SKU. Skipping.")
                continue

            upsert_variant_query = sql.SQL("""
                INSERT INTO {} (
                    var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val, 
                    price, cost, compare, upc, weight, weight_grams, published, status, 
                    debug_1, debug_2, debug_3, vendor
                )
                VALUES (
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                )
                ON CONFLICT (var_id) DO UPDATE SET 
                    var_image_url = EXCLUDED.var_image_url,
                    price = EXCLUDED.price, cost = EXCLUDED.cost, compare = EXCLUDED.compare,
                    upc = EXCLUDED.upc, weight = EXCLUDED.weight, published = EXCLUDED.published,
                    status = EXCLUDED.status, debug_1 = EXCLUDED.debug_1, debug_2 = EXCLUDED.debug_2,
                    debug_3 = EXCLUDED.debug_3;
            """).format(
                sql.Identifier(VARIANT_LOOKUP_TABLE),
                sql.Literal(var_sku),
                sql.Literal(product_id),
                sql.Literal(prepare_data_for_sql(variant.get("Handle", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Image Src", None))),
                sql.Literal(var_sku),
                sql.Literal(prepare_data_for_sql(variant.get("Option1 value", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Option2 value", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Option3 value", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Variant Price", None))),
                sql.Literal(prepare_data_for_sql(variant.get("cost", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Variant Compare At Price", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Variant Barcode", None))),
                sql.Literal(prepare_data_for_sql(variant.get("weight", None))),
                sql.Literal(prepare_data_for_sql(variant.get("weight_grams", None))),
                sql.Literal(prepare_data_for_sql(variant.get("published", None))),
                sql.Literal(status_to_set),
                sql.Literal(prepare_data_for_sql(variant.get("debug_1", None))),
                sql.Literal(prepare_data_for_sql(variant.get("debug_2", None))),
                sql.Literal(prepare_data_for_sql(variant.get("debug_3", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Vendor", None)))
            )
            cursor.execute(upsert_variant_query)

            

    # --- NEW PATH (Rule 4: Product does NOT exist, Insert everything) ---
    else:
        status_to_set = 'NEW'
        db_status = 'NEW'

        # 1. Product Table INSERT (RETURNING product_id)
        insert_product_query = sql.SQL("""
            INSERT INTO {} (
                cat, url, cat_name, title, image_url, descr, cert, opt_1, opt_2, opt_3, 
                tags, product_category, type, vendor, inventory_tracker, inventory_quantity, 
                debug_1, debug_2, debug_3, handle, status_int, status
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 
                {}, {}, {}, {}, {}, {}, 
                {}, {}, {}, {}, {}
            )
            RETURNING product_id;
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            sql.Literal(prepare_data_for_sql(product_data.get("cat", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("url", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cat_name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Title", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Image Src", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Body (HTML)", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("cert", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option1 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option2 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Option3 name", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("tags", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("product_category", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("type", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Vendor", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_tracker", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("inventory_quantity", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_1", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_2", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("debug_3", None))),
            sql.Literal(prepare_data_for_sql(product_data.get("Handle", None))),
            sql.Literal(status_to_set),
            sql.Literal(prepare_data_for_sql(product_data.get("Status", None)))
        )


        cursor.execute(insert_product_query)
        product_id = cursor.fetchone()[0] 
        
        # 2. Loop and INSERT Variants
        for variant in variants:
            var_sku = variant.get("Variant SKU")
            if not var_sku:
                print(f"Warning: Variant is missing SKU. Skipping.")
                continue

            insert_variant_query = sql.SQL("""
                INSERT INTO {} (
                    var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val, 
                    price, cost, compare, upc, weight, weight_grams, published, status_int, 
                    debug_1, debug_2, debug_3, vendor
                )
                VALUES (
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                );
            """).format(
                sql.Identifier(VARIANT_LOOKUP_TABLE),
                sql.Literal(var_sku),
                sql.Literal(product_id),
                sql.Literal(prepare_data_for_sql(variant.get("Handle", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Image Src", None))),
                sql.Literal(var_sku),
                sql.Literal(prepare_data_for_sql(variant.get("Option1 value", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Option2 value", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Option3 value", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Variant Price", None))),
                sql.Literal(prepare_data_for_sql(variant.get("cost", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Variant Compare At Price", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Variant Barcode", None))),
                sql.Literal(prepare_data_for_sql(variant.get("weight", None))),
                sql.Literal(prepare_data_for_sql(variant.get("weight_grams", None))),
                sql.Literal(prepare_data_for_sql(variant.get("published", None))),
                sql.Literal(status_to_set),
                sql.Literal(prepare_data_for_sql(variant.get("debug_1", None))),
                sql.Literal(prepare_data_for_sql(variant.get("debug_2", None))),
                sql.Literal(prepare_data_for_sql(variant.get("debug_3", None))),
                sql.Literal(prepare_data_for_sql(variant.get("Vendor", None)))
            )
            cursor.execute(insert_variant_query)

            
            
    return db_status, product_id

# --- MAIN DISPATCHER FUNCTION ---

def upsert_product_data(
    product_data: Dict[str, Any], 
    variants: List[Dict[str, Any]], 
    cursor
) -> Tuple[Optional[str], Optional[int]]:
    """
    DISPATCHER: Routes data to the single-variant or multi-variant handler.
    """
    print(product_data)
    print(variants)
    # --- Multi-Variant Product ---
    if variants:
        target_sku = variants[0].get("Variant SKU")
        if not target_sku:
            print("Error: Single-variant product is missing 'Variant SKU'. Aborting.")
            return None, None
            
        return upsert_multi_variant(product_data, variants, cursor)

    # --- Single-Variant Product ---
    else:
        target_sku = product_data.get("Variant SKU")
        if not target_sku:
            print("Error: Single-variant product is missing 'Variant SKU'. Aborting.")
            return None, None
            
        return upsert_single_variant(product_data, target_sku, cursor)


def debug(urls_stats, prod_stats):

    if not urls_stats:
        print("Source URL list (urls_stats) is empty.")
        return

    df_urls = pd.DataFrame(urls_stats)
    df_prod = pd.DataFrame(prod_stats)
    
    if 'url' not in df_prod['url']:
        print("Processed stats list (prod_stats) is missing the 'url' column.")
        missing_df = df_urls 
    else:
        missing_urls_mask = ~df_urls['url'].isin(df_prod['url'])
        missing_df = df_urls[missing_urls_mask].copy()

    if missing_df.empty:
        print("No debug file created.")
    else:
        missing_df.to_csv("../data/debug_missed_urls.csv", index=False)
        print(f"Found {len(missing_df)} missing URL(s). Written to debug_missed_urls.csv")


def create_url_handle(title, sku=None):
    if pd.isna(title) or title == '':
        return np.nan # Or some default handle if title is missing
    # Simple example: replace spaces with hyphens and lowercase
    handle_title = str(title).lower().replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')
    handle_sku = str(sku).lower().replace(' ', '_').replace(',', '') if pd.notna(sku) and sku != '' else ''
    if handle_sku:
        return f"{handle_title}_{handle_sku}"
    return handle_title

def extract_sku_from_shopify_meta(soup, var_name):
    """
    Locates the Shopify meta script tag and uses a regex to extract the SKU.
    """


    # 1. Find all script tags
    script_tags = soup.find_all('script')

    # 2. Iterate through scripts to find the one containing 'var meta = {'
    for script in script_tags:
        script_text = script.string
        if script_text and 'var meta = {' in script_text:

            # 3. Use regex to reliably extract the JSON content assigned to the 'meta' variable
            # We look for 'var meta = ' and capture everything up to the first semicolon ';'
            match = re.search(r'var\s+meta\s*=\s*(\{.*?\});', script_text, re.DOTALL)

            if match:
                # The captured group is the JSON string
                json_string = match.group(1).strip()
                
                try:
                    # 4. Parse the JSON string
                    data = json.loads(json_string)

                    # 5. Navigate the JSON structure to find the SKU
                    # Assuming the SKU is in the first variant (common for product pages)
                    if data.get('product') and data['product'].get('variants'):
                        variants = data['product']['variants']
                        for v in variants:
                            if var_name in v['public_title']:
                                return v['sku']
                        
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    return None

    return None



 
# Save product URL list to CSV 
def url_to_csv(products, filename='../data/matt_and_max_url.csv'):
            fieldnames = ['cat', 'url', 'name']
            file_exists = os.path.isfile(filename)
            
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(products)

# Save products to .CSV
def save_to_csv(products, filename='../data/mat_and_max__products.csv'):
            fieldnames = ['cat', 'num', 'brand', 'category', 'name', 'SKU', 'image_url', 'desc', 'upc', 'price', 'compare-at-price']
            file_exists = os.path.isfile(filename)

            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(products)
    
# Read product URL from CSV
def get_urls_csv(csv_file):
    product_urls = []
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        product_urls = list(reader)
    print(product_urls[1])
    return product_urls
    
# Webdriver settings
def setup_driver():
    # Settings
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--incognito')
    # User agent
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
        
    driver = webdriver.Chrome(options=options)
    return driver

def handle_cookie_banner(driver, timeout=5):
    """Checks for and clicks the cookie consent banner if present."""
    try:
        cookie_section_locator = (By.CLASS_NAME, "fancybox-outer")
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(cookie_section_locator)
        )
        print("Cookie consent banner found.")

        understand_button_locator = (By.CSS_SELECTOR, ".save-settings.btn")
        try:
            understand_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(understand_button_locator)
            )
            understand_button.click()
            print("Clicked 'I understand' on the cookie banner.")
            return True
        except TimeoutException:
            print("Could not find or click 'I understand' button.")


    except TimeoutException:
        print("Cookie consent banner not found within the timeout.")
        return False
    except Exception as e:
        print(f"An error occurred while handling the cookie banner: {e}")
        return False

# Get category page source


def fetch_cat_page(url, driver):
    driver.get(url)
    wait = WebDriverWait(driver, 50)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.footer__inner")))
        time.sleep(2)
        return driver.page_source
    except Exception as e:
        print(f"Error fetching URL")
        return None


def fetch_item_page(url, driver):
    try:
        driver.get(url)
        time.sleep(2)
        prev_last_product_link = ''
        while True:
            try:
                last_product = driver.find_elements(By.CSS_SELECTOR, ITEM)
                
                if prev_last_product_link == last_product[-1]:
                    break
                
                print(f'prev: {prev_last_product_link}')
                print(f'current: {last_product[-1]}')
                time.sleep(2)
                driver.execute_script("arguments[0].scrollIntoView(true);", last_product[-1])
                time.sleep(2)
                prev_last_product_link = last_product[-1]

                
            except Exception as e:
                print(f"PAGIN element not found or not clickable. Proceeding to get page source. {e}")
                break
        return driver.page_source
    except Exception as e:
        print(f"Error fetching URL '{url}': {e}")
        return None

def remove_dubl(file_name):
    df = pd.read_csv(file_name)
    df.drop_duplicates(subset=['page', 'url'], keep='first', inplace=True)
    df.to_csv(file_name, index=False)

def fetch_page(url, driver):
    MAX_RETRIES = 3

    for attempt in range(MAX_RETRIES):
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.footer__inner"))
            )
            return driver.page_source
        except ReadTimeoutError as e:
            print(f"ReadTimeoutError occurred: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {3} seconds...")
                time.sleep(3)
            else:
                print("Max retries reached. Failing.")
            # Re-raise the exception or handle the failure gracefully
                raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

def scrape_products_all():
    products = []

    # Get product sourse
    def fetch_page(url, driver):
        try:
            driver.get(url)
            time.sleep(2)
            return driver.page_source
        except:
            print("Error fetching")
            return None

    # Create product URL list
    def get_product_urls():
        pages = [URL_1, URL_2, URL_3, URL_4, URL_5, URL_6, URL_7, URL_8, URL_9, URL_10, URL_11, URL_12, URL_13, URL_14, URL_15, URL_16, URL_17, URL_18, URL_19, URL_20, URL_21, URL_22, URL_23, URL_24, URL_25, URL_26, URL_27, URL_28, URL_29, URL_30, URL_31, URL_32, URL_33, URL_34, URL_35, URL_36, URL_37, URL_38, URL_39, URL_40, URL_41, URL_42, URL_43, URL_44, URL_45, URL_46, URL_47]
        product_urls = []
        url_to_save = []
        urls_stats = []
        for page in pages:
            url = page
            html = fetch_page(url, driver)
            try:
                view_all_but = WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.hidden.sm\\:flex.border-l.border-gray-2.px-2.lg\\:px-4.flex.items-center"))
                )
                if view_all_but:
                    view_all_but.click()
                time.sleep(2)
            except:
                print("No view all button")
                pass
            html = driver.page_source
            soup = bs(html, "lxml") if html else print("No page source HTML")
            product_elements = soup.select(ITEM)
            for product in product_elements:
                product_page_element = product.select_one("a").get('href')
                product_urls.append({'cat': page, 'url': product_page_element})
                url_to_save.append({'cat': page, 'url': product_page_element})
                urls_stats.append({'cat': page, 'url': product_page_element})
            
            print(f'Found {len(url_to_save)} products on {page}')
            url_to_csv(url_to_save)
            url_to_save = []
        return product_urls, urls_stats

    # Parse single product data
    def parse_product(prod_soup, page, url):
            if prod_soup.select_one("div.flex-1.min-w-0.min-h-0.overflow-x-hidden"):
                data_bl = prod_soup.select_one("div.flex-1.min-w-0.min-h-0.overflow-x-hidden")
                
                if data_bl.select_one(NAME): # Get product name and parse SKU
                    name = data_bl.select_one(NAME).text.strip()
                    brand = data_bl.select_one("h2").text.strip()  
                else:
                    name = "N/A"
                    brand = "N/A"

                image_element = prod_soup.select(IMAGE) # Get product image URL
                img_list = []
                for a in image_element:
                    link = None
                    img_url_el = a.select_one("img")
                    img_url_list = img_url_el.get("data-srcset")
                    if img_url_list:
                        sources = img_url_list.split(',')
                        for source in sources:
                            parts = source.strip().split()
                            if len(parts) == 2:
                                url, descriptor = parts
                                if descriptor == '600w':
                                    link = url
                                    break # Found the link, no need to continue
                    img_list.append(link)
                v = [item for item in img_list if item is not None]    
                image_url = ','.join(v) if img_list else "N/A"
                
                ld_json_script = prod_soup.select('script[type="application/ld+json"]')
                sku = None
                if ld_json_script:
                    for a in ld_json_script:
                        try:
                            script_content = a.string
                            product_data = json.loads(script_content)
                            sku = product_data.get('sku')
                        except Exception as e:
                            print(f"An unexpected error occurred: {e}")

                desc = []
                desc_el = data_bl.select_one(str(DESC)) if data_bl.select(DESC) else "N/A" # Get product description
                desc.append(desc_el)

                cat = []
                cat_bl = prod_soup.select_one("ul.side-menu")
                if cat_bl:
                    cat.append([a.text.strip() for a in cat_bl.select("a.active")])
                else:
                    cat.append("N/A")
                
                price_bl = data_bl.select_one("div.font-body.tracking-normal.antialiased.mt-2")
                if price_bl:
                    price = price_bl.select_one("span").text.strip() if price_bl.select_one("span") else "N/A"
                    compare_at_price = price_bl.select_one("del").text.strip() if price_bl.select_one("del") else "N/A"  
                else:
                    price = "N/A"
                    compare_at_price = "N/A"
            
                if data_bl.select("div[class='md:mr-8']"):
                    video_bl = data_bl.select_one("div[class='md:mr-8']")
                    desc.append(video_bl)
                upc_bl = prod_soup.select('div[class="font-navigation uppercase tracking-tight sm:tracking-widest mt-2 mb-4 text-2xs sm:text-xs md:text-3xs xl:text-sm"]')
                if upc_bl:
                    upc = upc_bl[0].select_one('div').text.strip()
                else:
                    upc = ''
                
                handle = create_url_handle(name, sku)
                return { # Add product data as a single row
                    "cat": page,
                    "url": url,
                    "brand": brand,
                    "category": cat,
                    "Title": name,
                    "Variant SKU" : sku,
                    "Image Src": image_url,
                    "Body (HTML)": desc,
                    'Variant Barcode': upc,
                    'Variant Price': price,
                    'Variant Compare At Price': compare_at_price,
                    'Vendor': 'Matt and Max',
                    'Handle': handle
                }
            else:
                return { # Add product data as a single row
                    "cat": page,
                    "url": url,
                    "brand": '',
                    "category": '',
                    "Title": '',
                    "Variant SKU" : '',
                    "Image Src": '',
                    "Body (HTML)": '',
                    'Variant Barcode': '',
                    'Variant Price': '',
                    'Variant Compare At Price': '',
                    'Vendor': 'Matt and Max',
                    'Handle': ''
                }
    driver = setup_driver()
    url_count = 0
    product_count = 0
    stats = []
    prod_stats = []
    # open(PROD_DEBUG_FILE, 'w', encoding='utf-8').close()


    product_urls, urls_stats = get_product_urls()

    conn = None
    try:
        # 1. Establish the database connection
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Start a transaction
        cursor = conn.cursor()
        print('check1')
    except (Exception, psycopg2.Error) as error:
        print(f"Database Error: {error}", file=sys.stderr)
        if conn:
            conn.rollback() 
        sys.exit(1)
    

    # Parse product page
    for element in product_urls:
        cat = element['cat']
        url = element['url']
        print(f'Parsing product {url}')
            
        prod_html = fetch_page(url, driver)
        if prod_html:
            prod_soup = bs(prod_html, "lxml")
        if prod_soup:
            product = parse_product(prod_soup, cat, url)
        

        else:
            debug_message = f"Product {url} from {cat} is empty\n"
            print(f"WARNING: {debug_message.strip()}")
            with open(PROD_DEBUG_FILE, 'a', encoding='utf-8') as prod_debug_file:
                prod_debug_file.write(debug_message)
            continue

        if not product:
            continue 
        
        
        if product and product['Title']:
            db_status, product_id = upsert_product_data(product, [], cursor)
            conn.commit()
        
            product_count += 1
            prod_stats.append({'url': url, 'status': db_status, 'product_id': product_id, 'product_count': product_count})
    
    debug(urls_stats, prod_stats)
    stats.append({
        'product_count': product_count,
        'url_count': url_count,
    })
    stats_df = pd.DataFrame(stats)
    stats_df.to_csv('../data/matt_and_max_stats.csv', index=False)
    prod_stats_df = pd.DataFrame(prod_stats)
    prod_stats_df.to_csv('../data/matt_and_max_prod_stats.csv', index=False)

    driver.quit()
    if conn:
        conn.close()
        print("Database connection closed.")
    sys.exit(0)

# Main function
if __name__ == "__main__":
    products = scrape_products_all()
