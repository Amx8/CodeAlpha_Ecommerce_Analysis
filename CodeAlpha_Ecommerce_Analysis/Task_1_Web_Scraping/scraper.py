"""
E-Commerce Data Scraper
=======================
Scrapes product data from books.toscrape.com and generates
complementary relational tables for SQL/Power BI analysis.

Tables:
  1. categories   - Product categories
  2. products     - Scraped products (books)
  3. customers    - Synthetic customers
  4. reviews      - Product reviews (with text for sentiment analysis)
  5. orders       - Customer orders
  6. order_items  - Order line items
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import csv
import os
import time
import random
import re
from datetime import datetime, timedelta
from faker import Faker

# ─── Configuration ───────────────────────────────────────────────
BASE_URL = "https://books.toscrape.com"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(OUTPUT_DIR, "ecommerce.db")
DELAY = 0.3  # seconds between requests (be respectful)

fake = Faker()
Faker.seed(42)
random.seed(42)

# ─── Rating map ──────────────────────────────────────────────────
STAR_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

# ─── Review templates for sentiment analysis ─────────────────────
POSITIVE_REVIEWS = [
    "Absolutely loved this book! The writing style is captivating and the story kept me engaged from start to finish.",
    "One of the best purchases I've made. Highly recommend to anyone looking for a great read.",
    "Excellent quality and fast delivery. The content exceeded my expectations.",
    "This book changed my perspective on so many things. A must-read for everyone.",
    "Wonderful storytelling and beautiful prose. I couldn't put it down.",
    "Great value for the price. The book arrived in perfect condition.",
    "I've read this twice already and plan to read it again. Simply outstanding.",
    "The characters are well-developed and the plot is gripping. Five stars!",
    "Perfect gift for book lovers. My friend was thrilled to receive this.",
    "Superb writing and an unforgettable story. This is a masterpiece.",
    "Very well written and thought-provoking. I learned a lot from this book.",
    "Amazing storyline with unexpected twists. Kept me on the edge of my seat.",
    "Beautiful edition with great print quality. A joy to read and display.",
    "This author never disappoints. Another brilliant work of literature.",
    "Incredibly moving and powerful. This book touched my heart deeply.",
]

NEUTRAL_REVIEWS = [
    "The book was okay. Some parts were interesting but others felt slow.",
    "Decent read but nothing extraordinary. It's a standard book in its genre.",
    "Average quality. The story had potential but didn't fully deliver.",
    "It was fine for what it is. Not the best but certainly not the worst either.",
    "The book met my basic expectations. Nothing more, nothing less.",
    "Some chapters were great while others were forgettable. Mixed feelings overall.",
    "Readable but not memorable. I might recommend it if you have nothing else to read.",
    "The writing is competent but the story lacks originality. It's an average read.",
    "Fair price for an average book. The content is standard for this category.",
    "Not bad but I expected more based on the reviews. It's just okay.",
    "The book has its moments but overall it's rather predictable.",
    "Interesting concept but the execution could have been better.",
    "A middle-of-the-road read. Some good ideas but inconsistent quality.",
    "Took me a while to get into it. The second half is better than the first.",
    "It's a passable book. I neither loved it nor hated it.",
]

NEGATIVE_REVIEWS = [
    "Very disappointed with this purchase. The story was boring and predictable.",
    "Not worth the money. The writing quality is poor and full of errors.",
    "I couldn't finish this book. The plot makes no sense and the characters are flat.",
    "Terrible experience. The book arrived damaged and the content is mediocre at best.",
    "One of the worst books I've read this year. Save your money and buy something else.",
    "The description was misleading. The actual content is nothing like what was promised.",
    "Poorly written with numerous grammatical errors. Needs serious editing.",
    "I regret buying this. The story is unoriginal and the pacing is awful.",
    "Complete waste of time. There is no real plot or character development.",
    "Would not recommend. The book is overpriced for the quality you get.",
    "Extremely disappointing. The author clearly rushed through this work.",
    "The worst book in this series. It feels like a cash grab with no real effort.",
    "I wanted to like this book but it was just too poorly executed.",
    "Boring and uninspiring. I fell asleep multiple times trying to read it.",
    "Do not buy this. There are much better options available in this genre.",
]


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Scrape Categories
# ═══════════════════════════════════════════════════════════════════
def scrape_categories():
    """Scrape all book categories from the sidebar."""
    print("[1/6] Scraping categories...")
    resp = requests.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    categories = []
    nav = soup.find("ul", class_="nav-list")
    if nav:
        links = nav.find("ul").find_all("a")
        for idx, link in enumerate(links, start=1):
            name = link.text.strip()
            url = BASE_URL + "/" + link["href"]
            categories.append({
                "category_id": idx,
                "category_name": name,
                "category_url": url,
            })

    print(f"    Found {len(categories)} categories.")
    return categories


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Scrape Products (Books)
# ═══════════════════════════════════════════════════════════════════
def scrape_product_list():
    """Scrape the product listing pages (all 50 pages)."""
    print("[2/6] Scraping product listings...")
    products = []
    page = 1

    while True:
        if page == 1:
            url = f"{BASE_URL}/catalogue/page-{page}.html"
        else:
            url = f"{BASE_URL}/catalogue/page-{page}.html"

        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            break
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.find_all("article", class_="product_pod")

        if not articles:
            break

        for article in articles:
            title_tag = article.find("h3").find("a")
            title = title_tag["title"]
            detail_url = BASE_URL + "/catalogue/" + title_tag["href"].replace("../", "")

            price_text = article.find("p", class_="price_color").text.strip()
            price = float(re.sub(r"[^\d.]", "", price_text))

            rating_class = article.find("p", class_="star-rating")["class"][1]
            star_rating = STAR_MAP.get(rating_class, 0)

            in_stock = "In stock" in article.find("p", class_="instock").text

            products.append({
                "title": title,
                "price": price,
                "star_rating": star_rating,
                "in_stock": in_stock,
                "detail_url": detail_url,
            })

        print(f"    Page {page}: {len(articles)} products")
        page += 1
        time.sleep(DELAY)

    print(f"    Total products from listings: {len(products)}")
    return products


def scrape_product_details(products, categories):
    """Scrape individual product pages for UPC, description, category."""
    print("[3/6] Scraping product details (this may take a few minutes)...")
    cat_name_to_id = {c["category_name"]: c["category_id"] for c in categories}

    for i, product in enumerate(products):
        try:
            resp = requests.get(product["detail_url"], timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # UPC from product info table
            table = soup.find("table", class_="table-striped")
            rows = table.find_all("tr") if table else []
            upc = ""
            availability_count = 0
            for row in rows:
                header = row.find("th").text.strip()
                value = row.find("td").text.strip()
                if header == "UPC":
                    upc = value
                elif header == "Availability":
                    match = re.search(r"\d+", value)
                    availability_count = int(match.group()) if match else 0

            # Description
            desc_tag = soup.find("div", id="product_description")
            description = ""
            if desc_tag:
                p = desc_tag.find_next_sibling("p")
                if p:
                    description = p.text.strip()

            # Category from breadcrumb
            breadcrumb = soup.find("ul", class_="breadcrumb")
            cat_name = ""
            cat_id = None
            if breadcrumb:
                crumbs = breadcrumb.find_all("li")
                if len(crumbs) >= 3:
                    cat_name = crumbs[2].find("a").text.strip()
                    cat_id = cat_name_to_id.get(cat_name)

            # Image URL
            img_tag = soup.find("div", class_="item active")
            image_url = ""
            if img_tag and img_tag.find("img"):
                image_url = BASE_URL + "/" + img_tag.find("img")["src"].replace("../../", "")

            product["product_id"] = i + 1
            product["upc"] = upc
            product["description"] = description
            product["category_id"] = cat_id
            product["category_name"] = cat_name
            product["availability_count"] = availability_count
            product["image_url"] = image_url

        except Exception as e:
            print(f"    Warning: Could not scrape details for '{product['title']}': {e}")
            product["product_id"] = i + 1
            product["upc"] = ""
            product["description"] = ""
            product["category_id"] = None
            product["category_name"] = ""
            product["availability_count"] = 0
            product["image_url"] = ""

        if (i + 1) % 100 == 0:
            print(f"    Scraped {i + 1}/{len(products)} product details...")
        time.sleep(DELAY)

    print(f"    Completed scraping all {len(products)} product details.")
    return products


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Generate Complementary Tables
# ═══════════════════════════════════════════════════════════════════
def generate_customers(n=500):
    """Generate synthetic customer data."""
    print(f"[4/6] Generating {n} customers...")
    customers = []
    for i in range(1, n + 1):
        reg_date = fake.date_between(start_date="-3y", end_date="today")
        customers.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "country": fake.country(),
            "gender": random.choice(["Male", "Female"]),
            "age": random.randint(18, 70),
            "registration_date": reg_date.isoformat(),
        })
    return customers


def generate_reviews(products, customers, target_count=4000):
    """Generate product reviews with text suitable for sentiment analysis."""
    print(f"[5/6] Generating ~{target_count} reviews...")
    reviews = []
    review_id = 1
    customer_ids = [c["customer_id"] for c in customers]

    # Distribute reviews across products (weighted by star rating)
    for product in products:
        # Higher-rated products get more reviews
        num_reviews = random.randint(2, 8)
        for _ in range(num_reviews):
            if review_id > target_count:
                break

            # Review rating correlates with product rating but has variance
            base_rating = product["star_rating"]
            review_rating = max(1, min(5, base_rating + random.randint(-1, 1)))

            # Select review text based on rating (for sentiment analysis)
            if review_rating >= 4:
                review_text = random.choice(POSITIVE_REVIEWS)
            elif review_rating == 3:
                review_text = random.choice(NEUTRAL_REVIEWS)
            else:
                review_text = random.choice(NEGATIVE_REVIEWS)

            # Add product-specific context to make reviews more unique
            title_short = product["title"][:30]
            prefixes = [
                f"Regarding '{title_short}': ",
                f"About '{title_short}' - ",
                f"My review of '{title_short}': ",
                "",
                "",
                "",
            ]
            review_text = random.choice(prefixes) + review_text

            review_date = fake.date_between(start_date="-2y", end_date="today")

            reviews.append({
                "review_id": review_id,
                "product_id": product["product_id"],
                "customer_id": random.choice(customer_ids),
                "review_text": review_text,
                "star_rating": review_rating,
                "review_date": review_date.isoformat(),
                "helpful_votes": random.randint(0, 50),
                "verified_purchase": random.choice([True, False]),
            })
            review_id += 1

        if review_id > target_count:
            break

    print(f"    Generated {len(reviews)} reviews.")
    return reviews


def generate_orders(products, customers, target_count=2000):
    """Generate synthetic order data."""
    print(f"[6/6] Generating ~{target_count} orders and order items...")
    orders = []
    order_items = []
    order_item_id = 1

    statuses = ["Completed", "Completed", "Completed", "Shipped", "Processing", "Cancelled"]
    payment_methods = ["Credit Card", "PayPal", "Debit Card", "Bank Transfer"]

    for order_id in range(1, target_count + 1):
        customer = random.choice(customers)
        order_date = fake.date_between(start_date="-2y", end_date="today")
        status = random.choice(statuses)

        # Each order has 1-5 items
        num_items = random.choices([1, 2, 3, 4, 5], weights=[35, 30, 20, 10, 5])[0]
        selected_products = random.sample(products, min(num_items, len(products)))

        total = 0.0
        items_for_order = []
        for prod in selected_products:
            qty = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
            unit_price = prod["price"]
            line_total = round(qty * unit_price, 2)
            total += line_total

            items_for_order.append({
                "order_item_id": order_item_id,
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "line_total": line_total,
            })
            order_item_id += 1

        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_date": order_date.isoformat(),
            "total_amount": round(total, 2),
            "status": status,
            "payment_method": random.choice(payment_methods),
            "shipping_city": customer["city"],
            "shipping_country": customer["country"],
        })
        order_items.extend(items_for_order)

    print(f"    Generated {len(orders)} orders and {len(order_items)} order items.")
    return orders, order_items


# ═══════════════════════════════════════════════════════════════════
# Export Functions
# ═══════════════════════════════════════════════════════════════════
def export_to_csv(dataframes):
    """Export all tables as CSV files."""
    print("\n--- Exporting to CSV ---")
    for name, df in dataframes.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"    {name}.csv -> {len(df)} rows")


def export_to_sqlite(dataframes):
    """Export all tables to SQLite with proper types and foreign keys."""
    print("\n--- Exporting to SQLite ---")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    # Create tables with explicit schemas and foreign keys
    cursor.executescript("""
        CREATE TABLE categories (
            category_id   INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL
        );

        CREATE TABLE products (
            product_id         INTEGER PRIMARY KEY,
            title              TEXT NOT NULL,
            price              REAL NOT NULL,
            star_rating        INTEGER CHECK(star_rating BETWEEN 1 AND 5),
            in_stock           BOOLEAN,
            upc                TEXT,
            category_id        INTEGER,
            description        TEXT,
            availability_count INTEGER,
            image_url          TEXT,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        );

        CREATE TABLE customers (
            customer_id       INTEGER PRIMARY KEY,
            first_name        TEXT NOT NULL,
            last_name         TEXT NOT NULL,
            email             TEXT,
            city              TEXT,
            country           TEXT,
            gender            TEXT,
            age               INTEGER,
            registration_date TEXT
        );

        CREATE TABLE reviews (
            review_id         INTEGER PRIMARY KEY,
            product_id        INTEGER NOT NULL,
            customer_id       INTEGER NOT NULL,
            review_text       TEXT,
            star_rating       INTEGER CHECK(star_rating BETWEEN 1 AND 5),
            review_date       TEXT,
            helpful_votes     INTEGER DEFAULT 0,
            verified_purchase BOOLEAN,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );

        CREATE TABLE orders (
            order_id         INTEGER PRIMARY KEY,
            customer_id      INTEGER NOT NULL,
            order_date       TEXT,
            total_amount     REAL,
            status           TEXT,
            payment_method   TEXT,
            shipping_city    TEXT,
            shipping_country TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );

        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id      INTEGER NOT NULL,
            product_id    INTEGER NOT NULL,
            quantity      INTEGER NOT NULL,
            unit_price    REAL NOT NULL,
            line_total    REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );

        -- Indexes for common joins and queries
        CREATE INDEX idx_products_category ON products(category_id);
        CREATE INDEX idx_reviews_product ON reviews(product_id);
        CREATE INDEX idx_reviews_customer ON reviews(customer_id);
        CREATE INDEX idx_orders_customer ON orders(customer_id);
        CREATE INDEX idx_order_items_order ON order_items(order_id);
        CREATE INDEX idx_order_items_product ON order_items(product_id);
    """)

    # Insert data
    for name, df in dataframes.items():
        df.to_sql(name, conn, if_exists="append", index=False)
        print(f"    {name} -> {len(df)} rows inserted")

    conn.commit()

    # Verify relationships
    print("\n--- Verifying relationships ---")
    checks = [
        ("Products per category", "SELECT c.category_name, COUNT(p.product_id) as cnt FROM categories c LEFT JOIN products p ON c.category_id = p.category_id GROUP BY c.category_name ORDER BY cnt DESC LIMIT 5"),
        ("Reviews per product (top 5)", "SELECT p.title, COUNT(r.review_id) as cnt FROM products p LEFT JOIN reviews r ON p.product_id = r.product_id GROUP BY p.product_id ORDER BY cnt DESC LIMIT 5"),
        ("Orders per customer (top 5)", "SELECT c.first_name || ' ' || c.last_name as name, COUNT(o.order_id) as cnt FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id ORDER BY cnt DESC LIMIT 5"),
        ("Revenue by category (top 5)", "SELECT cat.category_name, ROUND(SUM(oi.line_total), 2) as revenue FROM order_items oi JOIN products p ON oi.product_id = p.product_id JOIN categories cat ON p.category_id = cat.category_id GROUP BY cat.category_name ORDER BY revenue DESC LIMIT 5"),
    ]

    for label, query in checks:
        print(f"\n    {label}:")
        for row in cursor.execute(query).fetchall():
            print(f"      {row}")

    conn.close()
    print(f"\n    Database saved to: {DB_PATH}")


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  E-Commerce Data Scraper & Database Builder")
    print("=" * 60)
    start_time = time.time()

    # Step 1: Scrape categories
    categories = scrape_categories()

    # Step 2-3: Scrape products
    products = scrape_product_list()
    products = scrape_product_details(products, categories)

    # Step 4-6: Generate complementary data
    customers = generate_customers(n=500)
    reviews = generate_reviews(products, customers, target_count=4000)
    orders, order_items = generate_orders(products, customers, target_count=2000)

    # Build DataFrames
    df_categories = pd.DataFrame(categories)[["category_id", "category_name"]]
    df_products = pd.DataFrame(products)[[
        "product_id", "title", "price", "star_rating", "in_stock",
        "upc", "category_id", "description", "availability_count", "image_url"
    ]]
    df_customers = pd.DataFrame(customers)
    df_reviews = pd.DataFrame(reviews)
    df_orders = pd.DataFrame(orders)
    df_order_items = pd.DataFrame(order_items)

    dataframes = {
        "categories": df_categories,
        "products": df_products,
        "customers": df_customers,
        "reviews": df_reviews,
        "orders": df_orders,
        "order_items": df_order_items,
    }

    # Export
    export_to_csv(dataframes)
    export_to_sqlite(dataframes)

    # Summary
    elapsed = time.time() - start_time
    total_records = sum(len(df) for df in dataframes.values())
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Total tables: {len(dataframes)}")
    print(f"  Total records: {total_records:,}")
    for name, df in dataframes.items():
        print(f"    - {name}: {len(df):,} rows x {len(df.columns)} columns")
    print(f"  Time elapsed: {elapsed:.1f} seconds")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"  Files: {len(dataframes)} CSVs + 1 SQLite database")
    print("=" * 60)


if __name__ == "__main__":
    main()
