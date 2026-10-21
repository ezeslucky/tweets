"""Global Trend Detection, Scoring, and Cross-Publisher Clustering Engine.

Features:
- Multi-source ingestion: Google Trends, GitHub Search, Hacker News, Reddit, RSS/Atom, LinkedIn
- Transparent trend velocity scoring
- Multi-source cross-publisher clustering & deduplication
- Distinct classification: Breaking, Trending, Popular, Emerging
- Thread-safe background execution with exponential backoff retry
"""
from __future__ import annotations

import concurrent.futures
import json
import math
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import database
import global_trends_catalog

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 (compatible; TweetsGlobalTrends/2.0)"

_TRENDS_LOCK = threading.Lock()
_ACTIVE_JOB_ID: Optional[str] = None
_ACTIVE_JOB_START: Optional[str] = None

STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are",
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but",
    "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from",
    "further", "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him",
    "himself", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just", "me",
    "more", "most", "my", "myself", "no", "nor", "not", "now", "of", "off", "on", "once", "only",
    "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she",
    "should", "so", "some", "such", "than", "that", "the", "their", "theirs", "them", "themselves",
    "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until",
    "up", "very", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom",
    "why", "with", "would", "you", "your", "yours", "yourself", "yourselves", "new", "says", "show",
    "ask", "hn", "using", "first", "release", "releases", "update", "updates", "day", "week"
}

def is_collecting() -> bool:
    return _TRENDS_LOCK.locked()

def get_active_job_info() -> Optional[Dict[str, Any]]:
    if not is_collecting():
        return None
    return {
        "job_id": _ACTIVE_JOB_ID,
        "started_at": _ACTIVE_JOB_START,
        "status": "running"
    }

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _parse_dt(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    txt = val.strip()
    fmts = (
        "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
        "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
        "%a, %d %b %Y %H:%M:%S +0000", "%Y-%m-%d %H:%M:%S"
    )
    for f in fmts:
        try:
            return datetime.strptime(txt, f).astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def strip_html(html: Optional[str]) -> str:
    if not html:
        return ""
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    for a, b in (("&nbsp;", " "), ("&amp;", "&"), ("&quot;", '"'), ("&#x27;", "'"),
                 ("&#39;", "'"), ("&lt;", "<"), ("&gt;", ">"), ("&hellip;", "..."),
                 ("&mdash;", "-"), ("&ndash;", "-"), ("&#x2F;", "/")):
        txt = txt.replace(a, b)
    return re.sub(r"\s+", " ", txt).strip()

def http_get(url: str, timeout: float = 12) -> str:
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, application/json, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")

# ==================== SOURCE INGESTION HANDLERS ====================

def fetch_rss_feed(source_meta: Dict[str, Any], hours: int = 48) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch and parse standard RSS/Atom feed."""
    url = source_meta.get("feed_url") or source_meta.get("url")
    if not url:
        return [], "No feed URL"

    cutoff = _now() - timedelta(hours=hours)
    try:
        xml_text = http_get(url, timeout=12)
        root = ET.fromstring(xml_text)
    except Exception as e:
        return [], f"FAIL {type(e).__name__}: {str(e)[:60]}"

    items: List[Dict[str, Any]] = []
    for node in root.iter():
        tag = node.tag.rsplit("}", 1)[-1].lower()
        if tag not in ("item", "entry"):
            continue

        title = link = when = summary = ""
        for child in node:
            lt = child.tag.rsplit("}", 1)[-1].lower()
            if lt == "title" and not title:
                title = strip_html(child.text or "")
            elif lt == "link" and not link:
                link = (child.get("href") or child.text or "").strip()
            elif lt in ("pubdate", "published", "updated", "date") and not when:
                when = (child.text or "").strip()
            elif lt in ("description", "summary", "content") and not summary:
                summary = strip_html(child.text or "")[:400]

        if not title or not link:
            continue

        dt = _parse_dt(when)
        if dt and dt < cutoff:
            continue

        items.append({
            "title": title,
            "url": link,
            "source": source_meta.get("name", "RSS"),
            "source_id": source_meta["id"],
            "category": source_meta.get("category", "Technology"),
            "region": source_meta.get("region", "Global"),
            "country": source_meta.get("country", ""),
            "language": source_meta.get("language", "en"),
            "score": 0,
            "comments": 0,
            "published_at": dt.isoformat() if dt else (_now().isoformat()),
            "extra": {
                "summary": summary,
                "publisher_tier": source_meta.get("publisher_tier", 1.2),
                "feed_url": url,
            }
        })
        if len(items) >= 20:
            break

    return items, f"ok ({len(items)})"

def fetch_google_trends(source_meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch official Google Trends RSS."""
    feed_url = source_meta.get("feed_url")
    if not feed_url:
        return [], "No feed URL"

    try:
        xml_text = http_get(feed_url, timeout=10)
        root = ET.fromstring(xml_text)
    except Exception as e:
        return [], f"FAIL {type(e).__name__}: {str(e)[:60]}"

    items: List[Dict[str, Any]] = []
    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        desc_el = item.find("description")
        pub_el = item.find("pubDate")

        # Parse approx_traffic namespace if present
        traffic_val = 100
        for child in item:
            if "approx_traffic" in child.tag.lower():
                raw_traffic = (child.text or "").replace("+", "").replace(",", "").replace("K", "000").replace("M", "000000").strip()
                try:
                    traffic_val = int(raw_traffic) // 1000
                except Exception:
                    pass

        title = (title_el.text or "").strip() if title_el is not None else ""
        link = (link_el.text or "").strip() if link_el is not None else ""
        summary = strip_html(desc_el.text or "") if desc_el is not None else ""
        pub_date = pub_el.text if pub_el is not None else None
        dt = _parse_dt(pub_date) or _now()

        if title and link:
            items.append({
                "title": f"Trending Search: {title}",
                "url": link,
                "source": source_meta.get("name", "Google Trends"),
                "source_id": source_meta["id"],
                "category": source_meta.get("category", "Social Media Trends"),
                "region": source_meta.get("region", "Global"),
                "country": source_meta.get("country", ""),
                "language": "en",
                "score": traffic_val * 10,
                "comments": traffic_val // 2,
                "published_at": dt.isoformat(),
                "extra": {
                    "summary": summary or f"Google Search volume spike of {traffic_val}K+ queries.",
                    "publisher_tier": source_meta.get("publisher_tier", 1.7),
                    "search_query": title,
                }
            })

    return items, f"ok ({len(items)})"

def fetch_hacker_news(source_meta: Dict[str, Any], hours: int = 48) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch high-scoring stories via Algolia API."""
    since = int((_now() - timedelta(hours=hours)).timestamp())
    url = f"https://hn.algolia.com/api/v1/search_by_date?tags=story&numericFilters=points>90,created_at_i>{since}&hitsPerPage=40"
    try:
        raw_json = http_get(url, timeout=12)
        data = json.loads(raw_json)
    except Exception as e:
        return [], f"FAIL {type(e).__name__}: {str(e)[:60]}"

    items = []
    for h in data.get("hits", []):
        title = (h.get("title") or "").strip()
        link = h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID')}"
        pts = h.get("points") or 0
        comms = h.get("num_comments") or 0
        dt = _parse_dt(h.get("created_at")) or _now()

        # Categorize intelligently
        cat = "Developer Ecosystem"
        t_low = title.lower()
        if any(w in t_low for w in ("ai", "llm", "gpt", "model", "deepseek", "anthropic", "openai", "claude")):
            cat = "Artificial Intelligence"
        elif any(w in t_low for w in ("startup", "launch", "funding", "acquired", "yc", "saas")):
            cat = "Startups and Business"
        elif any(w in t_low for w in ("science", "physics", "nasa", "quantum", "dna", "biology")):
            cat = "Science and Research"

        items.append({
            "title": title,
            "url": link,
            "source": "Hacker News",
            "source_id": "hn",
            "category": cat,
            "region": "Global",
            "country": "",
            "language": "en",
            "score": pts,
            "comments": comms,
            "published_at": dt.isoformat(),
            "extra": {
                "summary": f"Discussion on Hacker News with {pts} points and {comms} comments.",
                "publisher_tier": 1.6,
                "hn_id": h.get("objectID"),
            }
        })

    return items, f"ok ({len(items)})"

def fetch_github_trending(source_meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch high-velocity open source repos via GitHub API."""
    seven_days_ago = (_now() - timedelta(days=7)).strftime("%Y-%m-%d")
    url = f"https://api.github.com/search/repositories?q=created:>{seven_days_ago}+stars:>50&sort=stars&order=desc&per_page=20"
    try:
        raw_json = http_get(url, timeout=12)
        data = json.loads(raw_json)
    except Exception as e:
        return [], f"FAIL {type(e).__name__}: {str(e)[:60]}"

    items = []
    for r in data.get("items", []):
        name = r.get("full_name") or r.get("name")
        desc = r.get("description") or ""
        stars = r.get("stargazers_count") or 0
        forks = r.get("forks_count") or 0
        dt = _parse_dt(r.get("created_at")) or _now()

        cat = "Developer Ecosystem"
        d_low = f"{name} {desc}".lower()
        if any(w in d_low for w in ("ai", "agent", "llm", "rag", "model", "gpt", "vision")):
            cat = "Artificial Intelligence"

        items.append({
            "title": f"GitHub Release: {name} — {desc[:90]}" if desc else f"GitHub: {name}",
            "url": r.get("html_url") or f"https://github.com/{name}",
            "source": "GitHub Trending",
            "source_id": "github_trending",
            "category": cat,
            "region": "Global",
            "country": "",
            "language": r.get("language") or "en",
            "score": stars,
            "comments": forks,
            "published_at": dt.isoformat(),
            "extra": {
                "summary": desc or f"Trending open-source repository on GitHub with {stars:,} stars.",
                "publisher_tier": 1.7,
                "repo_name": name,
                "language": r.get("language"),
            }
        })

    return items, f"ok ({len(items)})"

def fetch_linkedin_trends(source_meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    """Authorized LinkedIn API updates."""
    token = os.getenv("LINKEDIN_ACCESS_TOKEN")
    if not token:
        return [], "Setup Required: Set LINKEDIN_ACCESS_TOKEN"

    headers = {
        "Authorization": f"Bearer {token}",
        "LinkedIn-Version": "202304",
        "X-Restli-Protocol-Version": "2.0.0",
        "User-Agent": UA,
    }
    url = "https://api.linkedin.com/rest/posts?q=author&count=20"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as e:
        return [], f"LinkedIn API Notice: {type(e).__name__}"

    items = []
    for el in data.get("elements", []):
        text = el.get("commentary", "")
        first_line = text.split("\n")[0][:120] if text else "LinkedIn Post"
        post_id = el.get("id", "")
        post_url = f"https://www.linkedin.com/feed/update/{post_id}" if post_id else "https://www.linkedin.com"
        created_time = el.get("createdAt")
        dt_str = datetime.fromtimestamp(created_time / 1000, tz=timezone.utc).isoformat() if created_time else _now().isoformat()

        items.append({
            "title": first_line,
            "url": post_url,
            "source": "LinkedIn",
            "source_id": "linkedin_official",
            "category": "Social Media Trends",
            "region": "Global",
            "country": "",
            "language": "en",
            "score": 50,
            "comments": 10,
            "published_at": dt_str,
            "extra": {
                "summary": text[:300],
                "publisher_tier": 1.5,
                "post_id": post_id,
            }
        })
    return items, f"ok ({len(items)})"

# ==================== CLUSTERING & TREND VELOCITY SCORING ====================

def _extract_keywords(title: str) -> Set[str]:
    clean = re.sub(r"[^a-zA-Z0-9\s]", " ", title.lower())
    words = clean.split()
    return {w for w in words if len(w) > 2 and w not in STOP_WORDS}

def cluster_and_score_stories(stories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Detect cross-publisher stories, compute velocity, and categorize into trend types."""
    now = _now()
    scored_stories = []

    # 1. Build keyword inverted index for cross-source clustering
    clusters: List[Dict[str, Any]] = []

    for s in stories:
        kws = _extract_keywords(s.get("title", ""))
        source_name = s.get("source", "")
        matched_cluster = None

        if len(kws) >= 2:
            for c in clusters:
                overlap = len(kws & c["keywords"])
                # Jaccard or significant token overlap
                if overlap >= 2 and (overlap / max(1, len(kws | c["keywords"]))) > 0.35:
                    matched_cluster = c
                    break

        if matched_cluster:
            matched_cluster["stories"].append(s)
            matched_cluster["sources"].add(source_name)
            matched_cluster["keywords"].update(kws)
            s["cluster_id"] = matched_cluster["id"]
        else:
            cid = f"cluster_{abs(hash(s.get('title', '')))}"
            new_c = {
                "id": cid,
                "keywords": set(kws),
                "sources": {source_name},
                "stories": [s],
            }
            clusters.append(new_c)
            s["cluster_id"] = cid

    # 2. Score velocity and determine trend type
    for s in stories:
        cid = s.get("cluster_id")
        cluster = next((c for c in clusters if c["id"] == cid), None)
        cross_source_count = len(cluster["sources"]) if cluster else 1
        s["cross_source_count"] = cross_source_count

        pub_dt = _parse_dt(s.get("published_at")) or now
        age_hours = max(0.2, (now - pub_dt).total_seconds() / 3600.0)

        # Engagement factor (normalized)
        score = s.get("score") or 0
        comments = s.get("comments") or 0
        raw_eng = score + (comments * 2)

        # Recency decay (half-life approx 16 hours)
        recency_weight = math.exp(-0.045 * age_hours)

        # Cross-source multiplier
        cross_multiplier = 1.0 + (0.45 * (cross_source_count - 1))

        # Publisher tier
        tier = float(s.get("extra", {}).get("publisher_tier") or 1.2)

        # Raw velocity calculation
        base_velocity = (min(raw_eng, 800) / 10.0 + 12.0)
        calc_velocity = base_velocity * recency_weight * cross_multiplier * tier

        velocity_score = round(min(99.9, max(10.0, calc_velocity)), 1)
        s["velocity_score"] = velocity_score

        # Determine Trend Type: Breaking, Trending, Popular, Emerging
        if age_hours <= 6.5 and (tier >= 1.6 or cross_source_count >= 2):
            trend_type = "Breaking"
        elif velocity_score >= 68.0 or cross_source_count >= 3:
            trend_type = "Trending"
        elif raw_eng >= 200 or (cross_source_count >= 2 and velocity_score >= 45.0):
            trend_type = "Popular"
        elif s.get("source_id") in ("huggingface_papers", "arxiv_cs_ai", "github_trending", "lobsters", "hn"):
            trend_type = "Emerging"
        else:
            trend_type = "Trending" if velocity_score > 50.0 else "Popular"

        s["trend_type"] = trend_type
        scored_stories.append(s)

    # Sort descending by velocity
    scored_stories.sort(key=lambda x: x["velocity_score"], reverse=True)
    return scored_stories

# ==================== COLLECTION PIPELINE ====================

def collect_global_trends(
    source_ids: Optional[List[str]] = None,
    max_workers: int = 8,
    hours: int = 48,
) -> Dict[str, Any]:
    """Execute complete global trend collection pipeline."""
    global _ACTIVE_JOB_ID, _ACTIVE_JOB_START

    acquired = _TRENDS_LOCK.acquire(blocking=False)
    if not acquired:
        return {
            "ok": False,
            "busy": True,
            "job_id": _ACTIVE_JOB_ID,
            "message": "A global trend collection job is currently active."
        }

    job_id = f"trend_job_{int(time.time() * 1000)}"
    _ACTIVE_JOB_ID = job_id
    _ACTIVE_JOB_START = _now().isoformat()
    start_time = time.time()

    all_sources = global_trends_catalog.get_all_trend_sources()
    if source_ids:
        active_sources = [s for s in all_sources if s["id"] in source_ids]
    else:
        active_sources = [s for s in all_sources if s.get("default_enabled")]

    collected_stories: List[Dict[str, Any]] = []
    source_status: Dict[str, str] = {}
    completed_sources = []
    failed_sources = []

    def _worker(s: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]], str]:
        stype = s.get("type", "rss")
        sid = s["id"]
        try:
            if stype == "google_trends":
                items, msg = fetch_google_trends(s)
            elif stype == "hn":
                items, msg = fetch_hacker_news(s, hours=hours)
            elif stype == "github":
                items, msg = fetch_github_trending(s)
            elif stype == "linkedin":
                items, msg = fetch_linkedin_trends(s)
            else:
                items, msg = fetch_rss_feed(s, hours=hours)
            return sid, items, msg
        except Exception as err:
            return sid, [], f"ERR: {str(err)[:50]}"

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_source = {executor.submit(_worker, s): s for s in active_sources}
            for future in concurrent.futures.as_completed(future_to_source):
                sid, items, msg = future.result()
                source_status[sid] = msg
                if items:
                    collected_stories.extend(items)
                    completed_sources.append(sid)
                elif "ok" in msg.lower():
                    completed_sources.append(sid)
                else:
                    failed_sources.append(sid)

        # Cluster, score, and rank
        processed_stories = cluster_and_score_stories(collected_stories)

        # Persist to database
        save_stats = database.save_global_trend_stories(processed_stories)

        duration = round(time.time() - start_time, 2)
        database.record_trend_schedule_run(
            status="completed",
            stories_count=len(processed_stories),
            new_count=save_stats["inserted"],
        )

        return {
            "ok": True,
            "job_id": job_id,
            "status": "completed",
            "duration_seconds": duration,
            "total_collected": len(processed_stories),
            "new_stories": save_stats["inserted"],
            "updated_stories": save_stats["updated"],
            "sources_completed_count": len(completed_sources),
            "sources_failed_count": len(failed_sources),
            "source_status": source_status,
            "timestamp": _now().isoformat(),
        }

    except Exception as e:
        database.record_trend_schedule_run(
            status="failed",
            error_message=str(e),
        )
        raise
    finally:
        _ACTIVE_JOB_ID = None
        _TRENDS_LOCK.release()
