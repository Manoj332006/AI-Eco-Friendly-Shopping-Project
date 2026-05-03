"""
database.py — SQLite setup + eco-product data pipeline
Sources: Open Food Facts, Open Beauty Facts, curated eco-brand seed data

Flow:
  1. First run → DB empty → seed with curated eco products + fetch from open APIs
  2. Every Sunday midnight → refresh new products
  3. User exhausts content → fetch more on demand
"""

import sqlite3
import httpx
import asyncio
import os
import re
import json
from datetime import datetime

DB_PATH       = os.getenv("DB_PATH", "data/ecocart.db")
INITIAL_BATCH = 300   # seed products per category on first run
REFRESH_BATCH = 100
BATCH_SIZE    = 5

# ── Curated eco-product seed data ─────────────────────────────────────────
# Real products from known sustainable brands — used on first run
SEED_PRODUCTS = [
    # BEAUTY & PERSONAL CARE
    {"name": "Solid Shampoo Bar", "brand": "Lush", "category": "beauty",
     "tags": ["plastic-free","zero-waste","vegan","natural","cruelty-free"],
     "description": "A concentrated solid shampoo bar that replaces 3 plastic bottles. Made with naturally derived ingredients, vegan, and cruelty-free. Lasts up to 80 washes.",
     "price": 12.99, "eco_score": 9.2, "certs": ["Vegan Society","Cruelty-Free"],
     "image_url": "https://images.unsplash.com/photo-1608248597279-f99d160bfcbc?w=400"},
    {"name": "Refillable Deodorant", "brand": "Wild", "category": "beauty",
     "tags": ["refillable","plastic-free","natural","vegan","cruelty-free","aluminum-free"],
     "description": "Aluminium-free natural deodorant with a refillable case. Each refill saves one plastic container. Scented with natural essential oils.",
     "price": 14.00, "eco_score": 9.0, "certs": ["Vegan","B-Corp"],
     "image_url": "https://images.unsplash.com/photo-1571019613454-1cb2f99b2d8b?w=400"},
    {"name": "Bamboo Toothbrush", "brand": "Humble", "category": "beauty",
     "tags": ["bamboo","biodegradable","plastic-free","natural-material","zero-waste"],
     "description": "Toothbrush with FSC-certified bamboo handle. The bristles are BPA-free nylon. Compostable handle, plastic-free packaging. One tree planted per purchase.",
     "price": 3.99, "eco_score": 8.8, "certs": ["FSC","1% for the Planet"],
     "image_url": "https://images.unsplash.com/photo-1607613009820-a29f7bb81c04?w=400"},
    {"name": "Zero-Waste Sunscreen SPF 50", "brand": "Stream2Sea", "category": "beauty",
     "tags": ["reef-safe","biodegradable","plastic-free","natural","vegan","zero-waste"],
     "description": "Reef-safe mineral sunscreen in biodegradable packaging. Free from oxybenzone and octinoxate. Certified biodegradable, safe for marine ecosystems.",
     "price": 18.99, "eco_score": 9.4, "certs": ["Reef-Safe Certified","Vegan"],
     "image_url": "https://images.unsplash.com/photo-1556228578-0d85b1a4d571?w=400"},
    {"name": "Organic Rose Face Oil", "brand": "Trilogy", "category": "beauty",
     "tags": ["organic","natural","vegan","cruelty-free","plant-based","botanical"],
     "description": "Cold-pressed certified organic rosehip oil. Sustainably sourced from Chilean rosehip seeds. Repairs, brightens and hydrates skin naturally.",
     "price": 24.99, "eco_score": 8.6, "certs": ["Organic Certified","Cruelty-Free"],
     "image_url": "https://images.unsplash.com/photo-1570194065650-d99fb4b38804?w=400"},
    {"name": "Beeswax Lip Balm", "brand": "Burt's Bees", "category": "beauty",
     "tags": ["beeswax","natural","cruelty-free","biodegradable","plastic-free"],
     "description": "100% natural lip balm with beeswax, vitamin E, and peppermint oil. Packaged in cardboard tube, fully recyclable. No parabens or phthalates.",
     "price": 4.49, "eco_score": 8.0, "certs": ["Natural Origin Certified"],
     "image_url": "https://images.unsplash.com/photo-1584308666744-24d5c474f2ae?w=400"},
    # HOME & CLEANING
    {"name": "Concentrated Cleaning Tablets", "brand": "Blueland", "category": "home",
     "tags": ["plastic-free","refillable","concentrated","zero-waste","vegan","non-toxic"],
     "description": "Just-add-water cleaning tablets — dissolve in a reusable bottle. Eliminates single-use plastic spray bottles entirely. Cruelty-free, EPA Safer Choice certified.",
     "price": 9.99, "eco_score": 9.5, "certs": ["EPA Safer Choice","Leaping Bunny"],
     "image_url": "https://images.unsplash.com/photo-1563453392212-326f5e854473?w=400"},
    {"name": "Beeswax Food Wraps", "brand": "Bee's Wrap", "category": "home",
     "tags": ["beeswax","plastic-free","compostable","reusable","natural","zero-waste"],
     "description": "Organic cotton infused with beeswax, tree resin, and jojoba oil. Replaces plastic cling wrap. Washable, reusable for up to a year, fully compostable.",
     "price": 15.99, "eco_score": 9.3, "certs": ["GOTS Organic","Compostable"],
     "image_url": "https://images.unsplash.com/photo-1542826438-bd32f43d626f?w=400"},
    {"name": "Compostable Dish Sponge", "brand": "Twist", "category": "home",
     "tags": ["compostable","plant-based","biodegradable","zero-waste","natural-material"],
     "description": "Made from plant-based cellulose and loofah. Fully compostable, no synthetic materials. Replaces petroleum-based sponges. 3-pack with zero plastic packaging.",
     "price": 7.99, "eco_score": 9.0, "certs": ["Compostable Certified","B-Corp"],
     "image_url": "https://images.unsplash.com/photo-1585771724684-38269d6639fd?w=400"},
    {"name": "Bamboo Paper Towels", "brand": "Who Gives A Crap", "category": "home",
     "tags": ["bamboo","recycled-material","plastic-free","sustainable","zero-waste"],
     "description": "Paper towels made from 100% bamboo — the fastest-growing plant on Earth. No inks, dyes or scents. 50% of profit donated to sanitation projects.",
     "price": 21.99, "eco_score": 8.9, "certs": ["FSC","B-Corp"],
     "image_url": "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=400"},
    {"name": "Laundry Strips", "brand": "Tru Earth", "category": "home",
     "tags": ["plastic-free","concentrated","zero-waste","vegan","hypoallergenic","waterless"],
     "description": "Pre-measured laundry detergent strips that dissolve completely. 94% smaller carbon footprint than liquid detergent. Fragrance-free, hypoallergenic, vegan.",
     "price": 19.99, "eco_score": 9.6, "certs": ["Vegan","Hypoallergenic Certified"],
     "image_url": "https://images.unsplash.com/photo-1545173168-9f1947eebb7f?w=400"},
    # FOOD & GROCERY
    {"name": "Organic Reusable Produce Bags", "brand": "Simple Ecology", "category": "food",
     "tags": ["organic","reusable","plastic-free","zero-waste","natural-material"],
     "description": "Set of 9 GOTS-certified organic cotton mesh bags for produce shopping. Replaces single-use plastic produce bags. Machine washable, built to last years.",
     "price": 13.99, "eco_score": 9.1, "certs": ["GOTS Organic"],
     "image_url": "https://images.unsplash.com/photo-1601598851547-4302969d0614?w=400"},
    {"name": "Organic Loose Leaf Green Tea", "brand": "Pukka", "category": "food",
     "tags": ["organic","fair-trade","plastic-free","natural","plant-based","sustainable"],
     "description": "Certified organic and Fairtrade loose-leaf green tea. Sustainably sourced from small-scale farmers. Compostable packaging, plastic-free sachets.",
     "price": 8.99, "eco_score": 9.0, "certs": ["Organic","Fairtrade","Plastic-Free"],
     "image_url": "https://images.unsplash.com/photo-1556679343-c7306c1976bc?w=400"},
    {"name": "Oat Milk Powder", "brand": "Minor Figures", "category": "food",
     "tags": ["plant-based","vegan","organic","low-carbon","sustainable"],
     "description": "Organic oat milk powder — just add water. 70% lower carbon footprint than dairy. Minimal packaging, no refrigeration needed. Barista quality.",
     "price": 11.99, "eco_score": 8.7, "certs": ["Organic","Vegan","Carbon Neutral"],
     "image_url": "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=400"},
    # FASHION & APPAREL
    {"name": "Organic Cotton T-Shirt", "brand": "Patagonia", "category": "clothing",
     "tags": ["organic","fair-trade","sustainable","natural-fiber","vegan","b-corp"],
     "description": "Made with 100% GOTS-certified organic cotton. Fair Trade Certified sewn. Patagonia donates 1% of sales to environmental causes.",
     "price": 39.00, "eco_score": 9.3, "certs": ["GOTS","Fair Trade","B-Corp","1% for Planet"],
     "image_url": "https://images.unsplash.com/photo-1521572163474-6864f9cf17ab?w=400"},
    {"name": "Hemp Linen Tote Bag", "brand": "Thought Clothing", "category": "accessories",
     "tags": ["hemp","natural-fiber","organic","vegan","sustainable-material","zero-waste"],
     "description": "Durable tote bag made from hemp-linen blend. Hemp requires no pesticides and regenerates soil. Replaces hundreds of single-use carrier bags.",
     "price": 22.00, "eco_score": 9.1, "certs": ["GOTS","Vegan"],
     "image_url": "https://images.unsplash.com/photo-1605488283688-8613f1e2d77e?w=400"},
    {"name": "Recycled Ocean Plastic Trainers", "brand": "Adidas x Parley", "category": "footwear",
     "tags": ["recycled-material","ocean-plastic","sustainable","upcycled","vegan"],
     "description": "Made with Parley Ocean Plastic — yarn spun from intercepted marine plastic waste. Each pair uses approximately 11 plastic bottles. Carbon-offset shipping.",
     "price": 89.99, "eco_score": 8.8, "certs": ["Parley Ocean Plastic","Carbon Neutral Shipping"],
     "image_url": "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=400"},
    # TECH & SOLAR
    {"name": "Solar Charging Power Bank", "brand": "BLAVOR", "category": "electronics",
     "tags": ["solar","renewable","energy-efficient","off-grid","sustainable","tech"],
     "description": "20,000mAh solar power bank with dual solar panels and wireless charging. Charges devices off-grid using clean solar energy. Recycled ABS casing.",
     "price": 45.99, "eco_score": 8.5, "certs": ["CE","RoHS"],
     "image_url": "https://images.unsplash.com/photo-1509391366360-2e959784a276?w=400"},
    {"name": "Recycled Plastic Laptop Stand", "brand": "Grovemade", "category": "electronics",
     "tags": ["recycled-material","upcycled","sustainable","tech","durable"],
     "description": "Laptop stand made from 100% post-consumer recycled plastic. Durable, scratch-resistant, disassembles for end-of-life recycling. Lifetime warranty.",
     "price": 55.00, "eco_score": 8.3, "certs": ["Recycled Content Certified"],
     "image_url": "https://images.unsplash.com/photo-1527864550417-7fd91fc51a46?w=400"},
    # OUTDOOR & GARDEN
    {"name": "Stainless Steel Water Bottle", "brand": "Klean Kanteen", "category": "outdoor",
     "tags": ["plastic-free","durable","reusable","zero-waste","long-lasting","stainless-steel"],
     "description": "18/8 food-grade stainless steel insulated bottle. Keeps drinks cold 24hr, hot 12hr. Lifetime warranty. Certified Climate Neutral brand.",
     "price": 32.00, "eco_score": 9.4, "certs": ["Climate Neutral","B-Corp"],
     "image_url": "https://images.unsplash.com/photo-1602143407151-7111542de6e8?w=400"},
    {"name": "Seed Paper Greeting Cards", "brand": "Botanical PaperWorks", "category": "gift",
     "tags": ["compostable","biodegradable","zero-waste","plant-based","gift","natural"],
     "description": "Greeting cards embedded with wildflower seeds — plant the card after reading. Made from 100% recycled paper pulp and flower seeds. Zero waste packaging.",
     "price": 6.99, "eco_score": 9.5, "certs": ["100% Recycled","Compostable"],
     "image_url": "https://images.unsplash.com/photo-1500462918059-b1a0cb512f1d?w=400"},
    {"name": "Bamboo Cutlery Set", "brand": "To-Go Ware", "category": "outdoor",
     "tags": ["bamboo","plastic-free","reusable","zero-waste","natural-material","portable"],
     "description": "Portable bamboo cutlery set with fork, knife, spoon, chopsticks and straw. Fits in included organic cotton pouch. Replaces disposable plastic cutlery.",
     "price": 11.99, "eco_score": 9.0, "certs": ["FSC Bamboo"],
     "image_url": "https://images.unsplash.com/photo-1547592166-23ac45744acd?w=400"},
    {"name": "Compost Bin Starter Kit", "brand": "OXO", "category": "home",
     "tags": ["zero-waste","compostable","sustainable","home","garden","natural"],
     "description": "Easy-start compost kit with countertop bin plus outdoor composter. Turns kitchen scraps into rich garden compost. Diverts food waste from landfill.",
     "price": 44.99, "eco_score": 9.2, "certs": [],
     "image_url": "https://images.unsplash.com/photo-1582735689369-4fe89db7114c?w=400"},
    {"name": "Organic Cotton Beeswax Candle", "brand": "Brooklyn Candle Studio", "category": "home",
     "tags": ["organic","natural","beeswax","non-toxic","sustainable","gift"],
     "description": "Hand-poured beeswax candles with organic cotton wicks. No paraffin, no synthetic fragrance. Amber glass jar is reusable after burning.",
     "price": 28.00, "eco_score": 8.7, "certs": ["Organic","Natural"],
     "image_url": "https://images.unsplash.com/photo-1596702846985-e5f46ad28d59?w=400"},
    {"name": "Recycled Yoga Mat", "brand": "Manduka", "category": "sport",
     "tags": ["recycled-material","sustainable","vegan","durable","natural","sport"],
     "description": "Yoga mat made from recycled rubber. PVC-free, latex-free, no toxic dyes. End-of-life take-back programme. Closed-cell surface is easy to clean.",
     "price": 68.00, "eco_score": 8.9, "certs": ["OEKO-TEX","Vegan","B-Corp"],
     "image_url": "https://images.unsplash.com/photo-1544367567-0f2fcb009e0b?w=400"},
]


# ── DB setup ──────────────────────────────────────────────────────────────
def get_db():
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id   TEXT    UNIQUE,
            name         TEXT    NOT NULL,
            brand        TEXT,
            category     TEXT,
            tag_list     TEXT,
            description  TEXT,
            price        REAL,
            eco_score    REAL,
            certifications TEXT,
            image_url    TEXT,
            product_url  TEXT,
            combined     TEXT,
            source       TEXT DEFAULT 'seed',
            created_at   TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            val TEXT
        );
    """)
    conn.commit()
    conn.close()
    print("DB initialised.")


def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    cats  = conn.execute(
        "SELECT category, COUNT(*) as c FROM products GROUP BY category"
    ).fetchall()
    conn.close()
    return {"total": total, "by_category": {r["category"]: r["c"] for r in cats}}


def set_meta(k, v):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, str(v)))
    conn.commit(); conn.close()


def get_meta(k):
    conn = get_db()
    row = conn.execute("SELECT val FROM meta WHERE key=?", (k,)).fetchone()
    conn.close()
    return row[0] if row else None


def _clean(text: str) -> str:
    text = str(text).lower()
    text = re.sub(r'[^a-z0-9\s\-]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _build_combined(p: dict) -> str:
    """name + brand + tags×2 + category + description → TF-IDF field"""
    tags_str = " ".join(p.get("tags", []))
    return _clean(
        p["name"] + " " + p.get("brand", "") + " " +
        (tags_str + " ") * 2 +
        p.get("category", "") + " " +
        p.get("description", "")
    )


def _upsert(conn, p: dict):
    conn.execute("""
        INSERT INTO products
            (product_id, name, brand, category, tag_list, description,
             price, eco_score, certifications, image_url, product_url, combined, source)
        VALUES
            (:product_id, :name, :brand, :category, :tag_list, :description,
             :price, :eco_score, :certifications, :image_url, :product_url, :combined, :source)
        ON CONFLICT(product_id) DO UPDATE SET
            name=excluded.name, description=excluded.description,
            tag_list=excluded.tag_list, eco_score=excluded.eco_score,
            price=excluded.price, combined=excluded.combined
    """, p)


def seed_database():
    """Insert curated seed products on first run."""
    conn  = get_db()
    added = 0
    for p in SEED_PRODUCTS:
        pid = re.sub(r'\s+', '-', (p["name"] + "-" + p["brand"]).lower())
        pid = re.sub(r'[^a-z0-9\-]', '', pid)
        try:
            _upsert(conn, {
                "product_id":    pid,
                "name":          p["name"],
                "brand":         p.get("brand", ""),
                "category":      p.get("category", "general"),
                "tag_list":      json.dumps(p.get("tags", [])),
                "description":   p.get("description", ""),
                "price":         p.get("price"),
                "eco_score":     p.get("eco_score", 7.0),
                "certifications": json.dumps(p.get("certs", [])),
                "image_url":     p.get("image_url", ""),
                "product_url":   "",
                "combined":      _build_combined(p),
                "source":        "seed",
            })
            added += 1
        except Exception as e:
            print(f"Seed error for {p['name']}: {e}")
    conn.commit()
    conn.close()
    print(f"Seeded {added} products.")
    return added


async def fetch_open_food_facts(pages: int = 5) -> int:
    """Fetch eco-tagged products from Open Food Facts API."""
    conn  = get_db()
    added = 0
    eco_tags_search = ["organic", "fair-trade", "vegan", "sustainable"]

    async with httpx.AsyncClient() as client:
        for tag in eco_tags_search[:2]:   # limit API calls
            for page in range(1, min(pages, 3) + 1):
                url = (
                    f"https://world.openfoodfacts.org/cgi/search.pl"
                    f"?action=process&tagtype_0=labels&tag_contains_0=contains"
                    f"&tag_0={tag}&page={page}&page_size=20&json=1"
                )
                try:
                    res  = await client.get(url, timeout=15.0)
                    data = res.json()
                    for item in data.get("products", []):
                        pid  = str(item.get("id") or item.get("_id", ""))
                        name = item.get("product_name", "").strip()
                        if not pid or not name or len(name) < 3: continue

                        desc = item.get("ingredients_text_en") or item.get("generic_name", "")
                        if not desc: continue

                        tags_raw = item.get("labels_tags", []) + item.get("categories_tags", [])
                        tags = list(set(
                            t.replace("en:", "").replace("-", " ").lower()
                            for t in tags_raw if len(t) < 40
                        ))[:10]
                        if not tags: tags = [tag]

                        brand = item.get("brands", "").split(",")[0].strip()
                        cat   = (item.get("categories", "").split(",")[0].strip() or "food").lower()[:30]

                        p = {
                            "product_id":    "off-" + pid[:50],
                            "name":          name[:100].title(),
                            "brand":         brand[:60],
                            "category":      cat,
                            "tag_list":      json.dumps(tags),
                            "description":   desc[:500],
                            "price":         None,
                            "eco_score":     7.5,
                            "certifications": json.dumps([]),
                            "image_url":     item.get("image_small_url", ""),
                            "product_url":   item.get("url", ""),
                            "combined":      _build_combined({
                                "name": name, "brand": brand, "tags": tags,
                                "category": cat, "description": desc
                            }),
                            "source": "openfoodfacts",
                        }
                        try:
                            _upsert(conn, p)
                            added += 1
                        except Exception:
                            pass
                    conn.commit()
                    await asyncio.sleep(0.5)
                except Exception as e:
                    print(f"Open Food Facts error: {e}")

    conn.close()
    print(f"Open Food Facts: {added} products added.")
    return added


def initial_fetch():
    """Sync wrapper — first run when DB is empty."""
    print("First run — seeding eco-products…")
    seed_database()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_open_food_facts(pages=3))
    loop.close()
    print("Initial fetch done.")


async def weekend_refresh() -> dict:
    print(f"[{datetime.now()}] Weekend refresh…")
    added = await fetch_open_food_facts(pages=5)
    return {"added": added}


async def fetch_more(extra: int = 5) -> int:
    return await fetch_open_food_facts(pages=extra)
