"""
user_profile.py — Per-user eco-preference tracking
Separate profiles for different product categories.
Tracks sustainability preferences, avoided materials, and eco-ratings.
"""

from collections import defaultdict


class UserProfile:

    def __init__(self):
        self.category_scores    = defaultdict(float)
        self.tag_scores         = defaultdict(float)
        self.seen               = set()
        self.liked              = set()
        self.disliked           = set()
        self.interaction_count  = 0
        self.intent_history     = []
        self.avoided_materials  = set()
        self.preferred_certs    = set()

    def record_interaction(self, product_id: str, tag_list: list, category: str,
                            liked: bool):
        weight = (1.5 if liked else -0.7) * min(1.0 + self.interaction_count * 0.05, 2.0)
        for tag in tag_list:
            self.tag_scores[tag.lower()] += weight
        self.category_scores[category.lower()] += (weight * 0.5)
        self.seen.add(product_id)
        (self.liked if liked else self.disliked).add(product_id)
        self.interaction_count += 1

    def mark_seen(self, ids: list):
        self.seen.update(ids)

    def record_intent(self, intent: str):
        if intent:
            self.intent_history.append(intent.lower())

    def score_item(self, tag_list: list, category: str = "") -> float:
        if self.interaction_count == 0:
            return 0.0
        raw = sum(self.tag_scores.get(t.lower(), 0.0) for t in tag_list)
        raw += self.category_scores.get(category.lower(), 0.0) * 0.3
        max_s = max((abs(v) for v in self.tag_scores.values()), default=1.0)
        return raw / max_s if max_s > 0 else raw

    def is_cold_start(self) -> bool:
        return self.interaction_count < 5

    def seen_set(self) -> set:
        return self.seen

    def top_tags(self, n: int = 5) -> list:
        return [t for t, s in sorted(self.tag_scores.items(),
                key=lambda x: x[1], reverse=True) if s > 0][:n]

    def bottom_tags(self, n: int = 3) -> list:
        return [t for t, s in sorted(self.tag_scores.items(),
                key=lambda x: x[1]) if s < 0][:n]

    def tag_score_map(self) -> dict:
        return dict(self.tag_scores)

    def interactions(self) -> int:
        return self.interaction_count

    def total_seen(self) -> int:
        return len(self.seen)

    def total_liked(self) -> int:
        return len(self.liked)

    def total_disliked(self) -> int:
        return len(self.disliked)

    def reset_seen(self):
        self.seen.clear()

    def full_reset(self):
        self.__init__()
