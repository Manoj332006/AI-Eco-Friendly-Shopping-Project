"""
EcoCart Backend — FastAPI
Run: uvicorn main:app --reload --reload-exclude "data/*"

On first run: seeds DB with curated eco-products + fetches from Open Food Facts
Every Sunday midnight: auto-refreshes product data
"""
# Add this import at the top with the other imports
from fastapi.staticfiles import StaticFiles
import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from database     import init_db, get_stats, initial_fetch, weekend_refresh, fetch_more
from preprocessor import load_products
from vectorizer   import build_tfidf
from recommender  import recommend
from user_profile import UserProfile

# ── Globals ───────────────────────────────────────────────────────────────
products_df = None
vectorizer  = None
tfidf_mat   = None
sessions: dict[str, UserProfile] = {}
scheduler = AsyncIOScheduler()

ADMIN_KEY = os.getenv("ADMIN_KEY", "ecocart-admin")


def get_user(sid: str) -> UserProfile:
    if sid not in sessions:
        sessions[sid] = UserProfile()
    return sessions[sid]


def load_all():
    global products_df, vectorizer, tfidf_mat
    products_df = load_products()
    if products_df is not None and not products_df.empty:
        vectorizer, tfidf_mat = build_tfidf(products_df, cache_path="data/tfidf_products.pkl")
    n = len(products_df) if products_df is not None else 0
    print(f"EcoCart ready: {n} products.")


async def _bg_reload():
    global products_df, vectorizer, tfidf_mat
    products_df = load_products()
    if products_df is not None and not products_df.empty:
        vectorizer, tfidf_mat = build_tfidf(
            products_df, "data/tfidf_products.pkl", force_rebuild=True
        )
    print("[BG] Products reloaded.")


async def _scheduled_refresh():
    print("=== Sunday midnight auto-refresh ===")
    await weekend_refresh()
    await _bg_reload()
    print("=== Auto-refresh complete ===")


# ── Lifespan ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("EcoCart starting…")
    init_db()
    stats = get_stats()
    print(f"DB: {stats['total']} products")

    if stats["total"] < 10:
        print("DB empty — seeding eco-products…")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, initial_fetch)

    load_all()

    scheduler.add_job(
        _scheduled_refresh,
        CronTrigger(day_of_week="sun", hour=0, minute=0),
        id="weekend_refresh", replace_existing=True
    )
    scheduler.start()
    print("EcoCart ready!")
    yield
    scheduler.shutdown()


app = FastAPI(title="EcoCart API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Models ────────────────────────────────────────────────────────────────
class RecommendRequest(BaseModel):
    session_id:   str
    intent_text:  Optional[str] = ""
    surprise_me:  bool = False
    top_n:        int = 12
    budget_level: Optional[str] = "any"   # "budget" | "any" | "premium"
    max_price:    Optional[float] = None
    category:     Optional[str] = None


class InteractionRequest(BaseModel):
    session_id: str
    product_id: str
    tag_list:   list[str]
    category:   str
    liked:      bool


# ── Routes ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    stats = get_stats()
    return {
        "status":   "ok",
        "products": stats["total"],
        "by_category": stats["by_category"],
        "sessions": len(sessions),
        "next_refresh": str(
            scheduler.get_job("weekend_refresh").next_run_time
            if scheduler.get_job("weekend_refresh") else "unknown"
        )
    }


@app.get("/categories")
def get_categories():
    if products_df is None or products_df.empty:
        return {"categories": []}
    cats = products_df["category"].value_counts().to_dict()
    return {"categories": [{"name": k, "count": v} for k, v in cats.items()]}


@app.post("/recommend")
async def get_recommendations(req: RecommendRequest, bg: BackgroundTasks):
    if products_df is None or products_df.empty:
        raise HTTPException(503, "Data not ready yet — please wait a moment.")

    user = get_user(req.session_id)
    df   = products_df.copy()

    # Category filter
    if req.category and req.category != "all":
        mask = df["category"].str.lower() == req.category.lower()
        if mask.sum() >= req.top_n:
            df = df[mask]

    # Align indices
    positions  = [i for i in df.index.tolist() if 0 <= i < tfidf_mat.shape[0]]
    sub_matrix = tfidf_mat[positions]
    df         = df.loc[positions].reset_index(drop=True)

    # Trigger background fetch if running low
    unseen = len(df) - len(user.seen_set() & set(df.index.tolist()))
    if unseen < req.top_n:
        bg.add_task(fetch_more, 5)
        bg.add_task(_bg_reload)

    results = recommend(
        df=df,
        tfidf_matrix=sub_matrix,
        vectorizer=vectorizer,
        user_profile=user,
        intent_text=req.intent_text,
        surprise_me=req.surprise_me,
        top_n=req.top_n,
        budget_level=req.budget_level or "any",
        max_price=req.max_price,
    )

    items = []
    for idx, row in results.iterrows():
        def safe(col):
            v = row.get(col, "")
            return str(v).strip() if v and pd.notna(v) and str(v).strip() not in ["nan","None"] else ""

        items.append({
            "product_id":     safe("product_id"),
            "name":           safe("name"),
            "brand":          safe("brand"),
            "category":       safe("category"),
            "tag_list":       row.get("tag_list") if isinstance(row.get("tag_list"), list) else [],
            "description":    safe("description"),
            "price":          float(row["price"]) if pd.notna(row.get("price")) else None,
            "eco_score":      float(row["eco_score"]) if pd.notna(row.get("eco_score")) else None,
            "certifications": row.get("certifications") if isinstance(row.get("certifications"), list) else [],
            "image_url":      safe("image_url"),
            "product_url":    safe("product_url"),
        })

    return {
        "results":       items,
        "is_cold_start": user.is_cold_start(),
        "top_tags":      user.top_tags(5),
        "tag_scores":    user.tag_score_map(),
        "stats": {
            "seen":         user.total_seen(),
            "liked":        user.total_liked(),
            "disliked":     user.total_disliked(),
            "interactions": user.interactions(),
        }
    }


@app.post("/interact")
def record_interaction(req: InteractionRequest):
    user = get_user(req.session_id)
    user.record_interaction(req.product_id, req.tag_list, req.category, req.liked)
    return {
        "ok":            True,
        "is_cold_start": user.is_cold_start(),
        "top_tags":      user.top_tags(5),
        "stats": {
            "seen":         user.total_seen(),
            "liked":        user.total_liked(),
            "disliked":     user.total_disliked(),
            "interactions": user.interactions(),
        }
    }


@app.post("/reset")
def reset_session(session_id: str):
    if session_id in sessions:
        sessions[session_id].full_reset()
    return {"ok": True}


@app.get("/profile/{session_id}")
def get_profile(session_id: str):
    user = get_user(session_id)
    return {
        "is_cold_start": user.is_cold_start(),
        "top_tags":      user.top_tags(5),
        "tag_scores":    user.tag_score_map(),
        "stats": {
            "seen":         user.total_seen(),
            "liked":        user.total_liked(),
            "disliked":     user.total_disliked(),
            "interactions": user.interactions(),
        }
    }
    



@app.post("/admin/refresh")
async def admin_refresh(admin_key: str = ""):
    if admin_key != ADMIN_KEY:
        raise HTTPException(403, "Forbidden")
    await weekend_refresh()
    await _bg_reload()
    return {"ok": True, "db": get_stats()}
    # This line must be LAST — after all @app.get/@app.post routes
app.mount("/", StaticFiles(directory=".", html=True), name="static")
