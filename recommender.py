"""
recommender.py — Core eco-product recommendation engine
Intent text → TF-IDF cosine similarity on (name + brand + tags×2 + category + description)
+ user personalisation based on eco-tag scores
"""

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from intent_parser import parse_intent, filter_by_intent, get_budget_boost
from vectorizer    import vectorize_intent

RESULT_COLS = [
    'product_id', 'name', 'brand', 'category', 'tag_list', 'description',
    'price', 'eco_score', 'certifications', 'image_url', 'product_url', 'final_score'
]


def recommend(df, tfidf_matrix, vectorizer, user_profile,
              intent_text=None, surprise_me=False, top_n=12,
              budget_level="any", max_price=None):
    """
    Main recommendation function.
    df has clean 0-based index aligned with tfidf_matrix rows.
    """
    seen = user_profile.seen_set()

    # ── DISCOVER (no intent) ──────────────────────────────────────────────
    if surprise_me or not intent_text or not intent_text.strip():
        cands = df[~df.index.isin(seen)].copy()
        if len(cands) == 0:
            user_profile.reset_seen()
            cands = df.copy()
        if max_price:
            price_mask = cands['price'].fillna(9999) <= max_price
            if price_mask.sum() >= top_n:
                cands = cands[price_mask]
        cands['final_score'] = cands['eco_score'].fillna(5.0) / 10.0
        cands = _budget_boost(cands, budget_level)
        cands = _user_boost(cands, user_profile)
        cands = _diversity(cands)
        results = cands.sort_values('final_score', ascending=False).head(top_n)
        user_profile.mark_seen(results.index.tolist())
        return _cols(results, df)

    # ── INTENT-BASED ──────────────────────────────────────────────────────
    user_profile.record_intent(intent_text)
    eco_tags = parse_intent(intent_text)

    # Step 1: filter by eco-tags
    cands = filter_by_intent(df, eco_tags)
    # Step 2: price filter
    if max_price:
        price_mask = cands['price'].fillna(9999) <= max_price
        if price_mask.sum() >= top_n:
            cands = cands[price_mask]
    # Step 3: exclude seen
    cands = cands[~cands.index.isin(seen)].copy()

    if len(cands) < top_n:
        cands = df[~df.index.isin(seen)].copy()
        if max_price:
            pm = cands['price'].fillna(9999) <= max_price
            if pm.sum() >= top_n:
                cands = cands[pm]
    if len(cands) < top_n:
        user_profile.reset_seen()
        cands = filter_by_intent(df, eco_tags)
        if len(cands) < top_n:
            cands = df.copy()

    # Step 4: TF-IDF cosine similarity
    intent_vec = vectorize_intent(intent_text, vectorizer)
    indices    = [i for i in cands.index.tolist() if 0 <= i < tfidf_matrix.shape[0]]
    if not indices:
        indices = list(range(min(top_n, tfidf_matrix.shape[0])))

    sim_scores = cosine_similarity(intent_vec, tfidf_matrix[indices]).flatten()
    cands      = df.loc[indices].copy()

    cands['intent_score'] = sim_scores
    cands['eco_norm']     = cands['eco_score'].fillna(5.0) / 10.0
    cands['user_score']   = cands['tag_list'].apply(
        lambda t: user_profile.score_item(t, cands.get('category', ''))
    )

    # Normalise
    for col in ['intent_score', 'eco_norm', 'user_score']:
        mx = cands[col].max()
        if mx > 0: cands[col] /= mx

    if user_profile.is_cold_start():
        cands['final_score'] = (
            0.60 * cands['intent_score'] +
            0.40 * cands['eco_norm']
        )
    else:
        cands['final_score'] = (
            0.50 * cands['intent_score'] +
            0.30 * cands['user_score'] +
            0.20 * cands['eco_norm']
        )

    cands = _dislike_penalty(cands, user_profile)
    cands = _budget_boost(cands, budget_level)
    cands = _diversity(cands)

    results = cands.sort_values('final_score', ascending=False).head(top_n)
    user_profile.mark_seen(results.index.tolist())
    return _cols(results, df)


# ── Helpers ────────────────────────────────────────────────────────────────

def _cols(results, df):
    cols = [c for c in RESULT_COLS if c in df.columns]
    return results[cols]


def _budget_boost(df, budget_level: str, boost: float = 0.05):
    from intent_parser import get_budget_boost
    boost_tags = get_budget_boost(budget_level)
    if not boost_tags: return df
    if 'final_score' not in df.columns:
        df['final_score'] = df['eco_score'].fillna(5.0) / 10.0
    df['final_score'] += df['tag_list'].apply(
        lambda t: boost if any(bt in [x.lower() for x in t] for bt in boost_tags) else 0.0
    )
    return df


def _user_boost(df, user_profile, boost: float = 0.06):
    top = user_profile.top_tags(5)
    if not top: return df
    if 'final_score' not in df.columns:
        df['final_score'] = df['eco_score'].fillna(5.0) / 10.0
    df['final_score'] += df['tag_list'].apply(
        lambda t: boost if any(tt in [x.lower() for x in t] for tt in top) else 0.0
    )
    return df


def _dislike_penalty(df, user_profile, penalty: float = 0.08):
    bad = user_profile.bottom_tags(3)
    if not bad: return df
    if 'final_score' not in df.columns:
        df['final_score'] = df['eco_score'].fillna(5.0) / 10.0
    df['final_score'] -= df['tag_list'].apply(
        lambda t: penalty * sum(1 for x in t if x.lower() in bad)
    )
    return df


def _diversity(df):
    """Penalise repeated categories — ensures variety."""
    if 'final_score' not in df.columns: return df
    seen_cats, penalty_map = set(), {}
    for idx in df.sort_values('final_score', ascending=False).index:
        cat     = str(df.loc[idx, 'category']).lower()
        overlap = 1 if cat in seen_cats else 0
        penalty_map[idx] = overlap * 0.02
        seen_cats.add(cat)
    df['final_score'] -= df.index.map(lambda i: penalty_map.get(i, 0))
    return df
