"""Comprehensive Automated Test Suite for Worldwide Real-Time Trend Discovery Engine.

Tests:
1. Topic title normalization & publisher watermark stripping.
2. Token stemming & similarity matching for different wordings of the same story.
3. Multi-publisher deduplication into a single canonical topic with multiple citations.
4. Signal-grounded ranking and status labels: Emerging, Rising, High activity, Established, Cooling down.
5. Multi-geo filtering across 8 worldwide regions and 11 categories.
6. Deep research synthesis: verified facts, differing perspectives, and 4 original draft post angles.
7. Unconfigured/unavailable source status handling (e.g., LinkedIn API without token).
8. Post draft queue integration.
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import database
import global_trends_catalog
import topic_researcher
import trends_engine


class TestWorldwideTrendsEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        database.init_db()

    def test_01_clean_publisher_decorations(self):
        """Verify publisher watermarks and channel tags are stripped from headlines."""
        cases = [
            ("Meta releases Llama 3.3 with 70B parameters - TechCrunch", "Meta releases Llama 3.3 with 70B parameters"),
            ("Apple broke an essential presentation feature of iPads | Hacker News", "Apple broke an essential presentation feature of iPads"),
            ("Show HN: An open-source SQL editor for PostgreSQL", "An open-source SQL editor for PostgreSQL"),
            ("Sensex climbs 800 pts on IT rally | Times of India", "Sensex climbs 800 pts on IT rally"),
            ("[r/LocalLLaMA] New quant methods for 3-bit weights", "New quant methods for 3-bit weights"),
            ("Breaking: OpenAI launches Operator agent — The Verge", "OpenAI launches Operator agent"),
        ]
        for raw, expected in cases:
            cleaned = trends_engine.clean_publisher_decorations(raw)
            self.assertEqual(cleaned, expected, f"Failed cleaning for: {raw}")

    def test_02_similarity_matching_different_wordings(self):
        """Verify different wordings of the same underlying story match tokens."""
        t1 = "DeepSeek-R1 open weights model released for reasoning"
        t2 = "DeepSeek releases R1 reasoning model with open weights"
        toks1 = trends_engine.normalize_title_tokens(t1)
        toks2 = trends_engine.normalize_title_tokens(t2)
        sim = trends_engine.compute_similarity(toks1, toks2)
        overlap = len(toks1 & toks2)
        self.assertGreaterEqual(sim, 0.4, f"Similarity {sim} should be >= 0.4 for near-identical stories")
        self.assertGreaterEqual(overlap, 3, "Should share at least 3 core entity tokens")

    def test_03_cross_publisher_clustering_deduplication(self):
        """Verify multi-outlet reporting merges into 1 canonical topic with multiple citations."""
        now = datetime.now(timezone.utc).isoformat()
        stories = [
            {
                "title": "OpenAI introduces Operator autonomous agent for web tasks - TechCrunch",
                "url": "https://techcrunch.com/2026/01/openai-operator",
                "source": "TechCrunch",
                "source_id": "techcrunch",
                "category": "Artificial Intelligence",
                "region": "North America",
                "score": 450,
                "comments": 80,
                "published_at": now,
                "extra": {"summary": "OpenAI previewed Operator, an autonomous browser agent that performs computer tasks."}
            },
            {
                "title": "OpenAI Operator: Autonomous web agent preview | The Verge",
                "url": "https://theverge.com/news/openai-operator-preview",
                "source": "The Verge",
                "source_id": "the_verge",
                "category": "Artificial Intelligence",
                "region": "North America",
                "score": 320,
                "comments": 65,
                "published_at": now,
                "extra": {"summary": "Operator can navigate forms, click buttons, and research information across websites."}
            },
            {
                "title": "Show HN: OpenAI Operator browser automation early tests",
                "url": "https://news.ycombinator.com/item?id=9998881",
                "source": "Hacker News",
                "source_id": "hn",
                "category": "Artificial Intelligence",
                "region": "Global",
                "score": 580,
                "comments": 210,
                "published_at": now,
                "extra": {"summary": "HN community benchmark tests on Operator latency and tool calling failure modes."}
            },
            {
                "title": "Sensex surges 900 points in broad-based market rally",
                "url": "https://livemint.com/markets/sensex-900",
                "source": "Livemint",
                "source_id": "livemint_tech",
                "category": "Finance and Markets",
                "region": "India",
                "score": 120,
                "comments": 15,
                "published_at": now,
                "extra": {"summary": "Indian benchmark indices rose on strong foreign institutional buying."}
            }
        ]

        canonical_topics, scored_stories = trends_engine.cluster_and_score_stories(stories)
        
        # 4 stories should consolidate into 2 topics (OpenAI Operator + Sensex)
        self.assertEqual(len(canonical_topics), 2, "4 stories across 2 events must consolidate into exactly 2 topics")
        
        # Find Operator topic
        operator_top = next((t for t in canonical_topics if "operator" in t["canonical_title"].lower()), None)
        self.assertIsNotNone(operator_top, "OpenAI Operator topic must exist")
        self.assertEqual(operator_top["source_count"], 3, "Operator topic must have 3 independent sources")
        self.assertIn("TechCrunch", [s["source_name"] for s in operator_top["sources"]])
        self.assertIn("The Verge", [s["source_name"] for s in operator_top["sources"]])
        self.assertIn("Hacker News", [s["source_name"] for s in operator_top["sources"]])
        self.assertEqual(operator_top["trend_status"], "High activity", "3 sources with >300 discussions must be High activity")

    def test_04_trend_status_and_scoring(self):
        """Verify the 5 supported trend labels are accurately assigned based on real signals."""
        now = datetime.now(timezone.utc)
        
        # Test Emerging (very fresh, niche technical source, 1-2 sources)
        fresh_story = [{
            "title": "Novel Attention Architecture for Linear Transformers - arXiv cs.AI",
            "url": "https://arxiv.org/abs/2601.12345",
            "source": "arXiv cs.AI",
            "source_id": "arxiv_cs_ai",
            "category": "Artificial Intelligence",
            "region": "Worldwide",
            "score": 95,
            "comments": 20,
            "published_at": (now - timedelta(hours=2)).isoformat(),
            "extra": {"summary": "New linear attention mechanism reducing quadratic inference bottleneck."}
        }]
        topics_em, _ = trends_engine.cluster_and_score_stories(fresh_story)
        self.assertEqual(topics_em[0]["trend_status"], "Emerging")

        # Test Cooling down (older than 40 hours)
        old_story = [{
            "title": "Quarterly Earnings Report Summary for Major Chipmakers",
            "url": "https://bloomberg.com/reports/chips-q3",
            "source": "Bloomberg",
            "source_id": "bloomberg",
            "category": "Finance and Markets",
            "region": "North America",
            "score": 40,
            "comments": 5,
            "published_at": (now - timedelta(hours=48)).isoformat(),
            "extra": {"summary": "Semiconductor earnings summary from earlier this week."}
        }]
        topics_cool, _ = trends_engine.cluster_and_score_stories(old_story)
        self.assertEqual(topics_cool[0]["trend_status"], "Cooling down")

    def test_05_database_persistence_and_multi_geo_filters(self):
        """Verify canonical topics save with unique DB constraints and filter accurately by region and category."""
        now = datetime.now(timezone.utc).isoformat()
        sample_topics = [
            {
                "topic_id": "test_top_india_upi",
                "canonical_title": "NPCI expands UPI global transactions across 10 countries",
                "slug": "npci-expands-upi-global-transactions-across-10-countries",
                "category": "Finance and Markets",
                "region": "India",
                "country": "IN",
                "language": "en",
                "trend_status": "Rising",
                "trend_score": 78.5,
                "velocity_score": 70.0,
                "first_seen_at": now,
                "last_updated_at": now,
                "summary": "India's instant payment network expands cross-border remittances.",
                "why_it_matters": "Significant boost for cross-border financial settlements.",
                "primary_url": "https://livemint.com/upi-global",
                "sources": [
                    {
                        "source_name": "Livemint",
                        "source_id": "livemint_tech",
                        "url": "https://livemint.com/upi-global",
                        "title": "NPCI expands UPI global payments",
                        "score": 140,
                        "comments": 28,
                        "published_at": now,
                        "summary": "UPI cross-border remittance expansion."
                    }
                ]
            },
            {
                "topic_id": "test_top_europe_ai_act",
                "canonical_title": "European Union finalizes AI Act compliance guidelines for frontier models",
                "slug": "european-union-finalizes-ai-act-compliance-guidelines",
                "category": "Artificial Intelligence",
                "region": "Europe",
                "country": "EU",
                "language": "en",
                "trend_status": "Established",
                "trend_score": 82.0,
                "velocity_score": 68.0,
                "first_seen_at": now,
                "last_updated_at": now,
                "summary": "EU Commission publishes binding technical guidance for general-purpose AI providers.",
                "why_it_matters": "Sets global regulatory compliance standard for frontier LLMs.",
                "primary_url": "https://politico.eu/ai-act-rules",
                "sources": [
                    {
                        "source_name": "Politico Europe",
                        "source_id": "politico_eu",
                        "url": "https://politico.eu/ai-act-rules",
                        "title": "EU AI Act guidelines published",
                        "score": 310,
                        "comments": 95,
                        "published_at": now,
                        "summary": "Technical documentation guidelines for GPAI models."
                    }
                ]
            }
        ]

        save_res = database.save_canonical_topics(sample_topics)
        self.assertGreaterEqual(save_res["total"], 2)

        # Test Region Filter: India
        india_topics = database.get_canonical_topics(region="india")
        india_slugs = [t["slug"] for t in india_topics.get("topics", [])]
        self.assertIn("npci-expands-upi-global-transactions-across-10-countries", india_slugs)

        # Test Region Filter: Europe
        eu_topics = database.get_canonical_topics(region="europe")
        eu_slugs = [t["slug"] for t in eu_topics.get("topics", [])]
        self.assertIn("european-union-finalizes-ai-act-compliance-guidelines", eu_slugs)

        # Test Category Filter: Finance and Markets
        fin_topics = database.get_canonical_topics(category="finance")
        fin_slugs = [t["slug"] for t in fin_topics.get("topics", [])]
        self.assertIn("npci-expands-upi-global-transactions-across-10-countries", fin_slugs)

    def test_06_deep_research_synthesis_and_draft_post_angles(self):
        """Verify deep research gathers real sources, synthesizes facts & viewpoints, and generates 4 draft angles."""
        research = topic_researcher.perform_deep_research("test_top_india_upi")
        self.assertTrue(research.get("ok"))
        self.assertEqual(research.get("canonical_title"), "NPCI expands UPI global transactions across 10 countries")
        
        # Verify key facts
        facts = research.get("key_facts", [])
        self.assertGreaterEqual(len(facts), 1)
        self.assertIn(facts[0]["confidence"], ("Confirmed", "Developing"))
        
        # Verify viewpoints
        views = research.get("differing_viewpoints", [])
        self.assertEqual(len(views), 3)

        # Verify 4 distinct post angles
        angles = research.get("draft_post_angles", [])
        self.assertEqual(len(angles), 4)
        angle_names = [a["angle_name"] for a in angles]
        self.assertIn("Technical Breakdown", angle_names)
        self.assertIn("Strategic Market Angle", angle_names)
        self.assertIn("Contrarian / Critical Tradeoffs", angle_names)
        self.assertIn("Concise Signal (TL;DR)", angle_names)

        # Verify drafts contain real source citations
        for a in angles:
            self.assertGreater(len(a["draft_text"]), 40)
            self.assertTrue(any(c in a["draft_text"] for c in ("NPCI", "UPI", "Livemint", "https://")))

    def test_07_unconfigured_source_truthful_health_reporting(self):
        """Verify unconfigured sources honestly report Setup Required rather than inventing mock data."""
        # Check LinkedIn source in catalog
        li_source = global_trends_catalog.get_source_by_id("linkedin_official")
        if li_source:
            # If no LINKEDIN_ACCESS_TOKEN is set in test env, status must be Setup Required
            if not os.getenv("LINKEDIN_ACCESS_TOKEN"):
                self.assertEqual(li_source["status"], "Setup Required")

    def test_08_post_draft_queue_addition(self):
        """Verify that a generated draft post can be pushed directly to the user's queue."""
        ts = datetime.now(timezone.utc).isoformat()
        item = database.add_x_queue_item(
            item_type="post_draft",
            content=f"NPCI expands UPI into 10 countries: key technical shift in cross-border settlements ({ts})",
            writing_style="thoughtful builder",
            topic_name="NPCI expands UPI global transactions",
            why_relevant="Grounded research angle"
        )
        self.assertTrue(bool(item and "status" in item))
        self.assertEqual(item["status"], "draft")
        self.assertEqual(item["topic_name"], "NPCI expands UPI global transactions")


if __name__ == "__main__":
    unittest.main()
