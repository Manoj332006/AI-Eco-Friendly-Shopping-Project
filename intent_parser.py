"""
intent_parser.py — Shopping intent → eco-tag mapping + description keywords for TF-IDF
Mirrors mood_parser.py from CineMatch.
"""

# Maps user intent/value phrases → product eco-tags used for filtering
INTENT_TAG_MAP = {
    "zero waste":       ["zero-waste", "package-free", "bulk", "reusable", "compostable"],
    "vegan":            ["vegan", "cruelty-free", "plant-based", "animal-free"],
    "organic":          ["organic", "natural", "chemical-free", "pesticide-free", "biodynamic"],
    "recycled":         ["recycled-material", "upcycled", "reclaimed", "post-consumer"],
    "fair trade":       ["fair-trade", "ethical-sourcing", "artisan", "community-made"],
    "energy saving":    ["energy-efficient", "solar", "low-power", "led", "renewable"],
    "plastic free":     ["plastic-free", "biodegradable", "compostable", "glass", "metal"],
    "sustainable":      ["sustainable", "eco-certified", "b-corp", "carbon-neutral"],
    "minimalist":       ["minimalist", "multi-use", "durable", "long-lasting", "simple"],
    "home":             ["home", "household", "kitchen", "cleaning", "bathroom"],
    "fashion":          ["clothing", "apparel", "fashion", "wear", "textile"],
    "beauty":           ["beauty", "skincare", "cosmetics", "personal-care", "grooming"],
    "food":             ["food", "grocery", "edible", "snack", "beverage", "supplement"],
    "tech":             ["tech", "electronics", "gadget", "device", "charger", "solar"],
    "outdoor":          ["outdoor", "garden", "camping", "hiking", "adventure", "sport"],
    "kids":             ["kids", "children", "baby", "toy", "school", "non-toxic"],
    "gift":             ["gift", "giftable", "artisan", "handmade", "premium"],
    "affordable":       ["budget-friendly", "affordable", "value", "accessible"],
    "premium":          ["premium", "luxury", "high-end", "artisan", "handcrafted"],
    "local":            ["local", "small-batch", "handmade", "regional", "artisan"],
    "carbon neutral":   ["carbon-neutral", "offset", "low-emission", "climate-positive"],
    "waterless":        ["waterless", "concentrated", "water-saving", "low-water"],
    "refillable":       ["refillable", "reusable", "refill", "concentrate", "subscription"],
    "natural":          ["natural", "organic", "plant-based", "botanical", "herbal"],
    "durable":          ["durable", "long-lasting", "lifetime-warranty", "repairable"],
    "secondhand":       ["secondhand", "vintage", "pre-owned", "refurbished", "thrifted"],
    "solar":            ["solar", "renewable", "off-grid", "energy-efficient"],
    "bamboo":           ["bamboo", "natural-material", "fast-growing", "renewable-resource"],
    "hemp":             ["hemp", "natural-fiber", "organic", "sustainable-material"],
    "linen":            ["linen", "natural-fiber", "breathable", "sustainable-material"],
    "beeswax":          ["beeswax", "natural", "sustainable", "plastic-free"],
    "compostable":      ["compostable", "biodegradable", "zero-waste", "natural-material"],
}

# Description-level keywords for richer TF-IDF product matching
INTENT_DESC_KEYWORDS = {
    "zero waste":     ["package-free", "zero packaging", "bulk refill", "no waste", "closed loop"],
    "vegan":          ["no animal", "cruelty free", "plant derived", "animal free", "vegan certified"],
    "organic":        ["certified organic", "pesticide free", "natural ingredients", "biodynamic", "no chemicals"],
    "recycled":       ["made from recycled", "post consumer", "upcycled material", "reclaimed", "second life"],
    "fair trade":     ["fair wages", "ethical labor", "artisan made", "community benefit", "trade certified"],
    "energy saving":  ["saves energy", "energy star", "low wattage", "solar powered", "renewable energy"],
    "plastic free":   ["no plastic", "plastic alternative", "glass packaging", "metal container", "natural wrap"],
    "sustainable":    ["sustainably sourced", "responsible", "eco certified", "green manufacturing", "b corp"],
    "minimalist":     ["one product does all", "multi purpose", "simple ingredients", "lasts years", "versatile"],
    "beauty":         ["skin care", "moisturiser", "cleanser", "serum", "natural beauty", "gentle formula"],
    "home":           ["household use", "cleaning", "kitchen", "home care", "non toxic formula"],
    "food":           ["organic ingredients", "whole food", "plant based nutrition", "clean label", "no additives"],
    "outdoor":        ["outdoor adventure", "hiking", "camping", "nature", "durable", "weather resistant"],
    "kids":           ["child safe", "non toxic", "BPA free", "age appropriate", "educational"],
    "durable":        ["lasts a lifetime", "built to last", "warranty", "repairable", "quality construction"],
    "refillable":     ["refill system", "reusable container", "concentrate formula", "less waste", "refill pouch"],
    "solar":          ["solar panel", "solar charging", "sun powered", "photovoltaic", "off grid"],
    "natural":        ["natural formula", "botanical extract", "herb infused", "plant powered", "earth derived"],
    "secondhand":     ["pre owned", "vintage", "restored", "like new", "refurbished quality"],
    "carbon neutral": ["carbon offset", "neutral emissions", "climate positive", "low footprint", "green shipping"],
}

# Category hints mapped from intent
INTENT_CATEGORY_MAP = {
    "beauty":   ["beauty", "skincare", "haircare", "cosmetics"],
    "fashion":  ["clothing", "apparel", "accessories", "footwear"],
    "home":     ["home", "kitchen", "cleaning", "furniture", "decor"],
    "food":     ["food", "grocery", "supplement", "beverage"],
    "tech":     ["electronics", "gadgets", "solar-tech", "accessories"],
    "outdoor":  ["outdoor", "garden", "sport", "travel"],
    "kids":     ["kids", "baby", "toys", "education"],
    "gift":     ["gift", "homeware", "beauty", "food", "accessories"],
}


def parse_intent(intent_text: str) -> list:
    """Returns eco-tag list for intent-based filtering."""
    lower = intent_text.lower()
    matched = set()
    for keyword in sorted(INTENT_TAG_MAP.keys(), key=len, reverse=True):
        if keyword in lower:
            matched.update(INTENT_TAG_MAP[keyword])
    if not matched:
        for word in lower.split():
            for kw, tags in INTENT_TAG_MAP.items():
                if word in kw or kw in word:
                    matched.update(tags)
    return list(matched)


def parse_category(intent_text: str) -> list:
    """Returns category hints from intent text."""
    lower = intent_text.lower()
    cats = []
    for kw, cat_list in INTENT_CATEGORY_MAP.items():
        if kw in lower:
            cats.extend(cat_list)
    return list(set(cats))


def get_description_keywords(intent_text: str) -> list:
    """Returns rich description keywords for TF-IDF vector enrichment."""
    lower = intent_text.lower()
    keywords = []
    for intent_key, kw_list in INTENT_DESC_KEYWORDS.items():
        if intent_key in lower or lower in intent_key:
            keywords.extend(kw_list)
    if not keywords:
        for word in lower.split():
            for intent_key, kw_list in INTENT_DESC_KEYWORDS.items():
                if word in intent_key or intent_key in word:
                    keywords.extend(kw_list)
    return list(set(keywords))


def filter_by_intent(df, eco_tags: list):
    """Filter DataFrame to products matching eco-tags."""
    if not eco_tags:
        return df
    mask = df['tag_list'].apply(
        lambda t: any(et.lower() in [x.lower() for x in t] for et in eco_tags)
    )
    filtered = df[mask]
    return filtered if len(filtered) >= 10 else df


def get_budget_boost(budget_level: str) -> list:
    """Returns boosted tags based on budget preference."""
    if budget_level == "budget":
        return ["budget-friendly", "affordable", "value", "accessible"]
    if budget_level == "premium":
        return ["premium", "artisan", "handcrafted", "luxury", "high-end"]
    return ["value", "sustainable", "durable"]
