"""
preprocessor.py — Load eco-product data from SQLite into DataFrame
Mirrors CineMatch preprocessor.py pattern.
"""

import pandas as pd
import json
import re
from database import get_db


def clean_text(text: str) -> str:
    text = str(text).lower()
    text = re.sub(r'[^a-z0-9\s\-]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def load_products() -> pd.DataFrame:
    conn = get_db()
    df   = pd.read_sql_query("SELECT * FROM products", conn)
    conn.close()

    if df.empty:
        print("Warning: No products in DB yet.")
        return pd.DataFrame()

    # Parse tag_list JSON
    df['tag_list'] = df['tag_list'].apply(
        lambda t: json.loads(t) if isinstance(t, str) and t.startswith('[') else []
    )

    # Parse certifications JSON
    df['certifications'] = df['certifications'].apply(
        lambda c: json.loads(c) if isinstance(c, str) and c.startswith('[') else []
    )

    # Numeric fields
    df['price']     = pd.to_numeric(df['price'],     errors='coerce')
    df['eco_score'] = pd.to_numeric(df['eco_score'], errors='coerce')
    df['eco_score'] = df['eco_score'].fillna(df['eco_score'].median())

    # Text cleanup
    df['name']        = df['name'].fillna('').str.strip()
    df['brand']       = df['brand'].fillna('').str.strip()
    df['description'] = df['description'].fillna('').str.strip()
    df['category']    = df['category'].fillna('general')
    df['combined']    = df['combined'].fillna('').apply(clean_text)
    df['image_url']   = df['image_url'].fillna('')
    df['product_url'] = df['product_url'].fillna('')

    # Drop empty descriptions, deduplicate by name+brand
    df = df[df['description'].str.len() > 10].copy()
    df = df.drop_duplicates(subset=['name', 'brand'], keep='first').reset_index(drop=True)

    # Popularity proxy: eco_score normalised
    df['popularity_score'] = df['eco_score'] / df['eco_score'].max()

    print(f"Loaded {len(df)} eco-products.")
    return df
