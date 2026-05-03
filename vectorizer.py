"""
vectorizer.py — TF-IDF builder + intent vectorizer for EcoCart
Mirrors CineMatch vectorizer.py pattern.
"""

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
import os


def build_tfidf(df, cache_path: str = "data/tfidf_cache.pkl", force_rebuild: bool = False):
    """Build TF-IDF matrix from combined product text. Atomic write."""
    if os.path.exists(cache_path) and not force_rebuild:
        with open(cache_path, 'rb') as f:
            cache = pickle.load(f)
        print(f"TF-IDF loaded from cache: {cache_path}")
        return cache['vectorizer'], cache['matrix']

    print(f"Building TF-IDF for {len(df)} products…")
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 3),
        max_features=15000,
        stop_words='english',
        min_df=1,
        sublinear_tf=True,
        strip_accents='unicode',
        analyzer='word',
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z0-9\-]{1,}\b",
    )
    matrix = vectorizer.fit_transform(df['combined'])

    os.makedirs(os.path.dirname(cache_path) if os.path.dirname(cache_path) else ".", exist_ok=True)
    tmp = cache_path + ".tmp"
    with open(tmp, 'wb') as f:
        pickle.dump({'vectorizer': vectorizer, 'matrix': matrix}, f)
    os.replace(tmp, cache_path)

    print(f"TF-IDF built: {matrix.shape[0]} x {matrix.shape[1]}")
    return vectorizer, matrix


def vectorize_intent(intent_text: str, vectorizer) -> np.ndarray:
    """
    Enrich intent text with description keywords for deeper eco-matching.
    Combined field = name + brand + tags×2 + category + description
    Intent vector  = intent×3 + desc_keywords×2
    This ensures cosine similarity captures VALUES not just category words.
    """
    from intent_parser import get_description_keywords
    keywords = get_description_keywords(intent_text)
    enriched = (intent_text + " ") * 3 + " ".join(keywords) + " " + " ".join(keywords)
    try:
        return vectorizer.transform([enriched])
    except Exception:
        return vectorizer.transform([intent_text])
