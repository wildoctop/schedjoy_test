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
from typing import Dict, Any, Tuple, Optional

load_dotenv()

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
CSV_READY = True
CSV = '../data/kbeauty_url.csv'
PROD_DEBUG_FILE = '../data/debug_kbeauty.log'
URL_DEBUG_FILE = '../data/debug_kbeauty_url.log'

# UPDATE WITH REAL DATA

ITEM = "h3[class='product-item__product-title fs-product-title ff-heading']" # Item CSS selector from list page


NAME = "h1[class='product__title ff-heading fs-heading-2-base']" # Product name CSS selector
DESC = "div[id='description']" # Product description CSS selector from product page

IMAGE = "div.product__media" # Product main image url CSS selector
CERT_BLOCK = "ul[class='pl-0 list-reset']" # Product certification CSS selector
CAT = 'a.collection-item__link'
BRAND = 'div[class="product__vendor fs-body-100"]'
#111




headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://google.com",
    "DNT": "0"
}

PRODUCT_TABLE = 'Product_DB'
VARIANT_LOOKUP_TABLE = 'Variant_DB'

# Note: I am assuming the connection and transaction (conn) is managed by the caller,
# and this function only executes queries and does NOT manage the connection itself.

def upsert_product_data(product_data: Dict[str, Any], cursor) -> Tuple[Optional[str], Optional[int]]:
    """
    Upserts product and variant data into PostgreSQL tables (Product_DB and Variant_DB).
    It expects the cursor to be managed by the calling function/context.
    """
    
    target_sku = product_data.get("Variant SKU")
    
    if not target_sku:
        print("Error: Product data is missing 'SKU'. Aborting.")
        return None, None

    select_query = sql.SQL("""
        SELECT product_id 
        FROM {} 
        WHERE sku = {}
    """).format(
        sql.Identifier(PRODUCT_TABLE),
        sql.Literal(target_sku)
    )
    
    cursor.execute(select_query)
    result = cursor.fetchone()
    
    product_id = result[0] if result else None
    
    # Placeholder for status value
    db_status = None
    
    # --- B. UPDATE LOGIC (If product_id exists) ---
    if product_id:
        status_to_set = 'UPD'

        # 1. Product Table UPDATE
        update_product_query = sql.SQL("""
            UPDATE {}
            SET 
                cat = {}, url = {}, cat_name = {}, title = {}, sku = {}, image_url = {}, 
                descr = {}, cert = {}, opt_1 = {}, opt_2 = {}, opt_3 = {}, tags = {}, 
                product_category = {}, type = {}, vendor = {}, inventory_tracker = {}, 
                inventory_quantity = {}, debug_1 = {}, debug_2 = {}, debug_3 = {},
                url_handle = {}, status = {} 
            WHERE product_id = {}
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            # Product Data
            sql.Literal(product_data.get("cat", "")),
            sql.Literal(product_data.get("url", "")), 
            sql.Literal(product_data.get("cat_name", "")),
            sql.Literal(product_data.get("name", "")),   # 'name' maps to 'title'
            sql.Literal(product_data.get("name", "")),
            sql.Literal(product_data.get("image_url", "")),
            sql.Literal(product_data.get("desc", "")),   # value from 'desc' goes into 'descr'
            sql.Literal(product_data.get("cert", "")),
            sql.Literal(product_data.get("opt_1", "")),
            sql.Literal(product_data.get("opt_2", "")),
            sql.Literal(product_data.get("opt_3", "")),
            sql.Literal(product_data.get("tags", "")),
            sql.Literal(product_data.get("product_category", "")),
            sql.Literal(product_data.get("type", "")),
            sql.Literal(product_data.get("vendor", "")),
            sql.Literal(product_data.get("inventory_tracker", "")),
            sql.Literal(product_data.get("inventory_quantity", "")), # Pass None for non-integer if conversion fails
            sql.Literal(product_data.get("debug_1", "")),  
            sql.Literal(product_data.get("debug_2", "")),
            sql.Literal(product_data.get("debug_3", "")),
            sql.Literal(product_data.get("handle", "")), 
            sql.Literal(status_to_set),
            # WHERE clause
            sql.Literal(product_id)
        )
        cursor.execute(update_product_query)

        # 2. Variant Table UPSERT (ON CONFLICT using var_is, which is set to target_sku)
        update_variant_query = sql.SQL("""
            INSERT INTO {} (
                var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val, 
                price, cost, compare, upc, weight, weight_grams, published, status, 
                debug_1, debug_2, debug_3
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, 
                COALESCE(NULLIF({}, ''), '0.00')::NUMERIC(10,2), 
                COALESCE(NULLIF({}, ''), '0.00')::NUMERIC(10,2),
                COALESCE(NULLIF({}, ''), '0.00')::NUMERIC(10,2),
                {}, {}, {}, {}, {}, {}, {}, {}
            )
            ON CONFLICT (var_id) DO UPDATE SET 
                var_image_url = EXCLUDED.var_image_url,
                price = EXCLUDED.price,
                cost = EXCLUDED.cost,
                compare = EXCLUDED.compare,
                upc = EXCLUDED.upc,
                weight = EXCLUDED.weight,
                published = EXCLUDED.published,
                status = EXCLUDED.status,
                debug_1 = EXCLUDED.debug_1,
                debug_2 = EXCLUDED.debug_2,
                debug_3 = EXCLUDED.debug_3;
        """).format(
            sql.Identifier(VARIANT_LOOKUP_TABLE),
            # Variant VALUES
            sql.Literal(target_sku), # var_is (Primary Key)
            sql.Literal(product_id),
            sql.Literal(product_data.get("handle", "")),
            sql.Literal(product_data.get("image_url", "")),
            sql.Literal(target_sku), # sku
            sql.Literal(product_data.get("opt_1_val", "")),
            sql.Literal(product_data.get("opt_2_val", "")),
            sql.Literal(product_data.get("opt_3_val", "")),
            sql.Literal(product_data.get("price", "")),
            sql.Literal(product_data.get("cost", "")),
            sql.Literal(product_data.get("compare", "")),
            sql.Literal(product_data.get("upc", "")),
            sql.Literal(product_data.get("weight", "")),
            sql.Literal(product_data.get("weight_grams", "")),
            sql.Literal(product_data.get("published", "")),
            sql.Literal(status_to_set),
            sql.Literal(product_data.get("debug_1", "")),
            sql.Literal(product_data.get("debug_2", "")),
            sql.Literal(product_data.get("debug_3", ""))
        )
        cursor.execute(update_variant_query)
        db_status = 'UPD'
    
    # --- C. INSERT LOGIC (If product_id does NOT exist) ---
    else:
        status_to_set = 'NEW'
        
        # 1. Product Table INSERT (Corrected column names and added missing ones)
        insert_product_query = sql.SQL("""
            INSERT INTO {} (
                cat, url, cat_name, title, sku, image_url, descr, cert, opt_1, opt_2, opt_3, 
                tags, product_category, type, vendor, inventory_tracker, inventory_quantity, 
                debug_1, debug_2, debug_3, url_handle, status
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 
                {}, {}, {}, {}, {}, {}, 
                {}, {}, {}, {}, {}
            )
            RETURNING product_id;
        """).format(
            sql.Identifier(PRODUCT_TABLE),
            # Product Values
            sql.Literal(product_data.get("cat", "")),
            sql.Literal(product_data.get("url", "")),
            sql.Literal(product_data.get("cat_name", "")),
            sql.Literal(product_data.get("name", "")),
            sql.Literal(product_data.get("name", "")),
            sql.Literal(product_data.get("image_url", "")),
            sql.Literal(product_data.get("desc", "")), # Data value that goes into 'descr' column
            sql.Literal(product_data.get("cert", "")),
            sql.Literal(product_data.get("opt_1", "")),
            sql.Literal(product_data.get("opt_2", "")),
            sql.Literal(product_data.get("opt_3", "")),
            sql.Literal(product_data.get("tags", "")),
            sql.Literal(product_data.get("product_category", "")),
            sql.Literal(product_data.get("type", "")),
            sql.Literal(product_data.get("vendor", "")),
            sql.Literal(product_data.get("inventory_tracker", "")),
            sql.Literal(product_data.get("inventory_quantity", "")),
            sql.Literal(product_data.get("brand", "")),
            sql.Literal(product_data.get("debug_2", "")),
            sql.Literal(product_data.get("debug_3", "")),
            sql.Literal(product_data.get("handle", "")),
            sql.Literal(status_to_set)
        )
        cursor.execute(insert_product_query)
        product_id = cursor.fetchone()[0] 
        
        # 2. Variant Table INSERT (Full column set)
        insert_variant_query = sql.SQL("""
            INSERT INTO {} (
                var_id, product_id, handle, var_image_url, sku, opt_1_val, opt_2_val, opt_3_val, 
                price, cost, compare, upc, weight, weight_grams, published, status, 
                debug_1, debug_2, debug_3
            )
            VALUES (
                {}, {}, {}, {}, {}, {}, {}, {}, 
                COALESCE(NULLIF({}, ''), '0.00')::NUMERIC(10,2), 
                COALESCE(NULLIF({}, ''), '0.00')::NUMERIC(10,2),
                COALESCE(NULLIF({}, ''), '0.00')::NUMERIC(10,2),
                {}, {}, {}, {}, {}, {}, {}, {}
            );
        """).format(
            sql.Identifier(VARIANT_LOOKUP_TABLE),
            # Variant Values
            sql.Literal(target_sku), # var_id (Primary Key)
            sql.Literal(product_id),
            sql.Literal(product_data.get("handle", "")),
            sql.Literal(product_data.get("image_url", "")),
            sql.Literal(target_sku), # sku
            sql.Literal(product_data.get("opt_1_val", "")),
            sql.Literal(product_data.get("opt_2_val", "")),
            sql.Literal(product_data.get("opt_3_val", "")),
            sql.Literal(product_data.get("price", "")),
            sql.Literal(product_data.get("cost", "")),
            sql.Literal(product_data.get("compare", "")),
            sql.Literal(product_data.get("upc", "")),
            sql.Literal(product_data.get("weight", "")),
            sql.Literal(product_data.get("weight_grams", "")),
            sql.Literal(product_data.get("published", "")),
            sql.Literal(status_to_set),
            sql.Literal(product_data.get("debug_1", "")),
            sql.Literal(product_data.get("debug_2", "")),
            sql.Literal(product_data.get("debug_3", ""))
        )
        cursor.execute(insert_variant_query)
        db_status = 'NEW'

    # --- D. Finalize ---
    # NOTE: Since the cursor is passed in, we assume the caller will commit/rollback and close.
    # The original commit/close logic is commented out to avoid closing the caller's resources.
    # conn.commit()
    # cursor.close() 
    return db_status, product_id


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

def extract_sku_from_shopify_meta_no_vars(soup):
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
                        return variants[0]['sku']
                        
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    return None

    return None


 
# Save product URL list to CSV 
def url_to_csv(products, filename='../data/kbeauty_url.csv'):
            fieldnames = ['cat', 'url', 'name']
            file_exists = os.path.isfile(filename)
            
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(products)

# Save products to .CSV
def save_to_csv(products, filename='../data/kbeauty_products.csv'):
            fieldnames = ['cat', 'name', 'SKU', 'image_url', 'desc', 'brand', 'price','upc','compare_price', 'opt_1', 'opt_1_val']
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

def fetch_page(url, driver):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.footer__inner"))
        ) 
        return driver.page_source
    except Exception as e:
        print(f"Error fetching URL")
        return None

# Main function that scrape all products
def scrape_products_all():
    products = []
    
    
    # Create product URL list
    def get_product_urls(driver, url_count):
        csv_file = CSV
        cat_urls = []
        urls_stats = []
        urls_to_save = []
        product_urls = []
        if CSV_READY:
            product_urls = get_urls_csv(csv_file)
        else:
            page = 1
            while True:
                html = fetch_cat_page(f"https://kbeauty.ca/collections?page={page}", driver)
                soup = bs(html, "lxml") if html else print("No page source HTML")
                cat_el = soup.select(CAT)
                if not cat_el:
                    break
                for cat in cat_el:
                    cat_url = cat.get('href')
                    cat_name_el = cat.select_one('div[class="collection-item__meta collection-item__title ff-heading fs-body-100"]')
                    product_count_span = cat.select_one('span.collection-item__product-count')
                    if product_count_span:
                        product_count_span.decompose()
                    cat_name = cat_name_el.text.strip() 
                cat_urls.append({'type': 'https://kbeauty.ca/collections', 'url': f"https://kbeauty.ca{cat_url}", 'name': cat_name})
                page += 1
            for cat in cat_urls:
                url = cat['url']
                html = fetch_item_page(url, driver)
                soup = bs(html, "lxml") if html else print("No page source HTML")
                prod_el = soup.select(ITEM)
                for el in prod_el:
                    url_count += 1
                    product_urls.append({'cat': url, 'url': f"https://kbeauty.ca{el.select_one('a').get('href')}", 'name': cat['name']})
                    urls_to_save.append({'cat': url, 'url': f"https://kbeauty.ca{el.select_one('a').get('href')}", 'name': cat['name']})
                    urls_stats.append({'cat': url, 'url': f"https://kbeauty.ca{el.select_one('a').get('href')}", 'name': cat['name']})
                print(f'Found {len(product_urls)} products')
                url_to_csv(urls_to_save)
                urls_to_save = []
            url = 'https://kbeauty.ca/collections/makeup-korean'
            html = fetch_item_page(url, driver)
            soup = bs(html, "lxml") if html else print("No page source HTML")
            prod_el = soup.select(ITEM)
            for el in prod_el:
                product_urls.append({'cat': url, 'url': f"https://kbeauty.ca{el.select_one('a').get('href')}", 'name': 'MakeUp'})
                urls_to_save.append({'cat': url, 'url': f"https://kbeauty.ca{el.select_one('a').get('href')}", 'name': 'MakeUp'})
                urls_stats.append({'cat': url, 'url': f"https://kbeauty.ca{el.select_one('a').get('href')}", 'name': cat['name']})
            print(f'Found {len(product_urls)} products')
            url_to_csv(urls_to_save)
            urls_to_save = []
        return product_urls, urls_stats

    # Parse single product data
    def parse_product(prod_soup, cat, url, cat_name):
            if prod_soup:
                name = prod_soup.select_one(NAME).text.strip()
                

                if prod_soup.select('div[class="media media--has-lightbox"]'):
                    time.sleep(1)
                    image_url_el = []
                    image_url = []
                    image_url_el = prod_soup.select('div[class="media media--has-lightbox"]')
                    print(f'Found {len(image_url_el)} images')
                    
                    for el in image_url_el:
                        if el.select_one('img'):
                            img = el.select_one('img')  
                            new_url = re.sub(r"&width=.*", "&width=1000", img.get('src'))
                            image_url.append(new_url)
                    image_url = set(image_url)
                else:
                    image_url = ""

               

                if prod_soup.select(DESC):
                    desc = prod_soup.select_one(DESC).text.strip()
                else:
                    desc = ""

                sku = extract_sku_from_shopify_meta_no_vars(prod_soup)
                #upc = extract_barcode_from_json_script(prod_soup)
                
                if prod_soup.select('span[data-price]'):
                    price = prod_soup.select_one('span[data-price]').text.strip()
                else:
                    price = ""
                
                compare_price = ""
                if prod_soup.select('s[data-compare-price]'):
                    compare_price = prod_soup.select_one('s[data-compare-price]').text.strip()
                



            return { # Add product data as a single row

                "cat_name": cat_name,
                "Title": name,
                "Variant SKU" : sku,
                "Image Src": image_url,
                "Body (HTML)": desc,
                "Variant Barcode": sku,
                "Variant Image": '',
                "Variant Price": price,
                "Variant Compare At Price": compare_price,
                "Vendor": "KBeauty",
                "Option1 name": "",
                "Option1 value": "",

            }
    # Parse product variant data if any
    
    def parse_variant(url, driver):
        driver.get(url)

        variants_data= []



        var_block = driver.find_elements(By.CSS_SELECTOR, "div[class='product__controls-group product__variants-wrapper product__block product__block--medium']")
        buttons = var_block[0].find_elements(By.CSS_SELECTOR, "button[type='button']")
        options = var_block[0].find_elements(By.CSS_SELECTOR, "select[id='option1']")

        if buttons:
            print("len(buttons) = ", len(buttons))
            print("buttons = ", buttons)
            
            html = driver.page_source
            var_soup = bs(html, 'lxml')

  

            if var_soup.select('div[class="image aspect-ratio--square animation--image animation--lazy-load loaded"]'):
                image_url_el = var_soup.select('div[class="image aspect-ratio--square animation--image animation--lazy-load loaded"]')
                img = image_url_el[0].select_one('img')
                new_url = re.sub(r"&width=.*", "&width=1000", img.get('src'))
                var_image_url = new_url
            else:
                var_image_url = ""

            span_element = var_soup.select_one('span[data-selected-value-for-option]')
            if span_element:
                button_value = span_element.get_text(strip=True)
            else:
                button_value = "N/A"

            button_name = var_soup.select_one('label[for="option1"]')
            if button_name:
                span_element.decompose()
                button_name = button_name.get_text(strip=True)
            else:
                button_name = "N/A"
            
            var_sku = extract_sku_from_shopify_meta(var_soup, button_value)
            #var_upc = extract_barcode_from_json_script(var_soup)
                
            if var_soup.select('span[data-price]'):
                var_price = var_soup.select_one('span[data-price]').text.strip()
            else:
                var_price = ""
            
            var_compare_price = ""
            if var_soup.select('s[data-compare-price]'):
                var_compare_price = var_soup.select_one('s[data-compare-price]').text.strip()

            var_to_add = {}
            var_to_add['cat_name'] = ''
            var_to_add['Title'] = ''
            var_to_add['Variant SKU'] = var_sku
            var_to_add['Image Src'] = var_image_url
            var_to_add['Body (HTML)'] = ''
            var_to_add['Variant Barcode'] = var_sku
            var_to_add['Variant Image'] = ''
            var_to_add['Variant Price'] = var_price
            var_to_add['Variant Compare At Price'] = var_compare_price
            var_to_add['Vendor'] = ""
            var_to_add['Option1 name'] = button_name
            var_to_add['Option1 value'] = button_value
            variants_data.append(var_to_add)
           

            for i in range(1, len(buttons)):
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class='product__controls-group product__variants-wrapper product__block product__block--medium']"))
                )
                
                var_block = driver.find_elements(By.CSS_SELECTOR, "div[class='product__controls-group product__variants-wrapper product__block product__block--medium']")
                buttons_list = var_block[0].find_elements(By.CSS_SELECTOR, "button[type='button']")
                cross = buttons_list[i].find_elements(By.CSS_SELECTOR, "span[class='product__chip-crossed']")
                
                if i <= len(buttons_list) and cross:
                    try:
                        buttons_list[i].click()
                    except Exception as e:
                        close = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Close dialog']")
                        close[0].click()
                        time.sleep(2)
                        buttons_list[i].click()
                
                    time.sleep(3) 
                        
                    html = driver.page_source
                    var_soup = bs(html, 'lxml')

                    

                    if var_soup.select('div[class="image aspect-ratio--square animation--image animation--lazy-load loaded"]'):
                        image_url_el = var_soup.select('div[class="image aspect-ratio--square animation--image animation--lazy-load loaded"]')
                        img = image_url_el[0].select_one('img')
                        new_url = re.sub(r"&width=.*", "&width=1000", img.get('src'))
                        var_image_url = new_url
                    else:
                        var_image_url = ""

                    span_element = var_soup.select_one('span[data-selected-value-for-option]')
                    if span_element:
                        button_value = span_element.get_text(strip=True)
                    else:
                        button_value = "N/A"

                    button_name = var_soup.select_one('label[class="product__label fs-body-100"]')
                    if button_name:
                        span_element.decompose()
                        button_name = button_name.get_text(strip=True)
                    else:
                        button_name = "N/A"
                    var_sku = extract_sku_from_shopify_meta(var_soup, button_value)
                        
                    #var_upc = extract_barcode_from_json_script(var_soup)
                            
                    if var_soup.select('span[data-price]'):
                        var_price = var_soup.select_one('span[data-price]').text.strip()
                    else:
                        var_price = ""
                        
                    var_compare_price = ""
                    if var_soup.select('s[data-compare-price]'):
                        var_compare_price = var_soup.select_one('s[data-compare-price]').text.strip()

                    var_to_add = {}
                    var_to_add['cat_name'] = ''
                    var_to_add['Title'] = ''
                    var_to_add['Variant SKU'] = var_sku
                    var_to_add['Image Src'] = var_image_url
                    var_to_add['Body (HTML)'] = ''
                    var_to_add['Variant Barcode'] = var_sku
                    var_to_add['Variant Image'] = ''
                    var_to_add['Variant Price'] = var_price
                    var_to_add['Variant Compare At Price'] = var_compare_price
                    var_to_add['Vendor'] = ""
                    var_to_add['Option1 name'] = button_name
                    var_to_add['Option1 value'] = button_value
                    variants_data.append(var_to_add)
                else:
                    continue
                        
     

        elif options:
            print("options")
            options_list = None
            try:
                options_list_el = driver.find_element(By.CSS_SELECTOR, 'select[id="option1"]')
                options_list_el.click()
                time.sleep(1)
                print('list was opened')
                options_list = options_list_el.find_elements(By.CSS_SELECTOR, 'option')
                print(f'Found {len(options_list)} options')
            except:
                print("No options list click")

            # Get option list
            if options_list:
                for i in range(len(options_list)):
                    option_text = options_list[i].get_attribute('text').strip()
                    if "Unavailable" not in option_text:
                        options_list = options_list_el.find_elements(By.CSS_SELECTOR, 'option')
                        print('check 1')
                        try:
                            option_text = options_list[i].get_attribute('text').strip()
                            options_list[i].click()
                            try:
                                WebDriverWait(driver, 10).until(EC.staleness_of(options_list_el))
                            except TimeoutException:
                                print("Page did not refresh as expected.")

                            html = driver.page_source
                            var_soup = bs(html, 'lxml')

                            
                            print('check 2')
                            if var_soup.select('div[class="image aspect-ratio--square animation--image animation--lazy-load loaded"]'):
                                image_url_el = var_soup.select('div[class="image aspect-ratio--square animation--image animation--lazy-load loaded"]')
                                img = image_url_el[0].select_one('img')
                                new_url = re.sub(r"&width=.*", "&width=1000", img.get('src'))
                                var_image_url = new_url
                            else:
                                var_image_url = ""

                            span_element = var_soup.select_one('span[data-selected-value-for-option]')
                            if span_element:
                                option_value = span_element.get_text(strip=True)
                            else:
                                option_value = "N/A"

                            option_name = var_soup.select_one('label[class="product__label fs-body-100"]')
                            if option_name:
                                span_element.decompose()
                                option_name = option_name.get_text(strip=True)
                            else:
                                option_name = "N/A"
                                
                            var_sku = extract_sku_from_shopify_meta(var_soup, option_value)
                            #var_upc = extract_barcode_from_json_script(var_soup)
                                    
                            print('check 3')
                            if var_soup.select('span[data-price]'):
                                var_price = var_soup.select_one('span[data-price]').text.strip()
                            else:
                                var_price = ""
                                
                            var_compare_price = ""
                            if var_soup.select('s[data-compare-price]'):
                                var_compare_price = var_soup.select_one('s[data-compare-price]').text.strip()

                            print('check 4')
                            var_to_add = {}
                            var_to_add['cat_name'] = ''
                            var_to_add['Title'] = ''
                            var_to_add['Variant SKU'] = var_sku
                            var_to_add['Image Src'] = var_image_url
                            var_to_add['Body (HTML)'] = ''
                            var_to_add['Variant Barcode'] = var_sku
                            var_to_add['Variant Image'] = ''
                            var_to_add['Variant Price'] = var_price
                            var_to_add['Variant Compare At Price'] = var_compare_price
                            var_to_add['Vendor'] = ""
                            var_to_add['Option1 name'] = option_name
                            var_to_add['Option1 value'] = option_value
                            variants_data.append(var_to_add)
                        except:
                            print("No option click")

                        # Open option list
                        if i != len(options_list) - 1:
                            time.sleep(1)
                            try:
                                options_list_el = driver.find_element(By.CSS_SELECTOR, 'select[id="option1"]')
                                options_list_el.click()
                                print('list was opened')
                            except:
                                print("No options list click")
        return variants_data

    driver = setup_driver()
    url_count = 0
    product_count = 0
    stats = []
    prod_stats = []
    open(PROD_DEBUG_FILE, 'w', encoding='utf-8').close()


    product_urls, urls_stats = get_product_urls(driver, url_count)

    conn = None
    try:
        # 1. Establish the database connection
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Start a transaction
        cursor = conn.cursor()
    except (Exception, psycopg2.Error) as error:
        print(f"Database Error: {error}", file=sys.stderr)
        if conn:
            conn.rollback() 
        sys.exit(1)
    

    # Parse product page
    for element in product_urls:
        cat = element['cat']
        url = element['url']
        name = element['name']
        print(f'Parsing product {url}')
            
        prod_html = fetch_page(url, driver)
        if prod_html:
            prod_soup = bs(prod_html, "lxml")
        if prod_soup:
            product = parse_product(prod_soup, cat, url, name)
        

        else:
            debug_message = f"Product {url} from {cat} is empty\n"
            print(f"WARNING: {debug_message.strip()}")
            with open(PROD_DEBUG_FILE, 'a', encoding='utf-8') as prod_debug_file:
                prod_debug_file.write(debug_message)
            continue

    
        
        if prod_soup.select("div[class='product__controls-group product__variants-wrapper product__block product__block--medium']"):
            
            variants = parse_variant(url, driver)
            print(variants) 
            product['Variant SKU'] = ''
            product['Variant Price'] = ''
            product['Variant Compare At Price'] = ''
            product["Option1 name"] = variants[0]['Option1 name']
            product['Vendor'] = ""
            for variant in variants:
                variant['Option1 name'] = ""
        
            product_to_save = [product]
            product_to_save.extend(variants)
    
        else:
            product_to_save = [product]
        db_status, product_id = upsert_product_data(product, cursor)
        product_count += 1
        prod_stats.append({'url': url, 'status': db_status, 'product_id': product_id, 'product_count': product_count})
    
    debug(urls_stats, prod_stats)
    stats.append({
        'product_count': product_count,
        'url_count': url_count,
    })
    stats.to_csv('../data/kbeauty_stats.csv', index=False)

    driver.quit()
    if conn:
        conn.close()
        print("Database connection closed.")
    sys.exit(0)

# Main function
if __name__ == "__main__":
    products = scrape_products_all()
