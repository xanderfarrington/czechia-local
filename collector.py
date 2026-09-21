# -*- coding: utf-8 -*-
"""
Czech Republic Media Tracker — Collector + Translator + Topic Splitter

Keeps current behavior:
- rolling latest window in data/articles_latest.json
- topic files in data/articles_<topic>.json

Adds:
- persistent archive in data/articles_archive.json
- stronger dedupe / redundancy cleanup
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
import feedparser
import pandas as pd

# ================= CONFIG =================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

TIMEOUT = 20
MAX_RETRIES = 3
RETRY_SLEEP = 2.0

KEEP_HOURS = int(os.getenv("KEEP_HOURS", "48"))

OUT_JSON = Path(os.getenv("OUT_JSON", "data/articles_latest.json"))
ARCHIVE_JSON = Path(os.getenv("ARCHIVE_JSON", "data/articles_archive.json"))

SOURCES: Dict[str, Dict] = {
    # Domestic-focused feeds: articles are treated as Czech-related even when
    # the headline/lead does not explicitly say "Czechia" or "Czech Republic".
    "iROZHLAS - Domestic": {
        "feeds": ["https://www.irozhlas.cz/rss/irozhlas/section/zpravy-domov"],
        "assume_czech": True,
    },
    "České noviny / ČTK - Czech Republic": {
        "feeds": ["https://www.ceskenoviny.cz/sluzby/rss/cr.php"],
        "assume_czech": True,
    },

    # Broader Czech outlets. These feeds can contain foreign stories, so the
    # country relevance filter is applied to their title/lead text.
    "ČT24": {
        "feeds": ["https://ct24.ceskatelevize.cz/rss"],
        "assume_czech": False,
    },
    "Novinky": {
        "feeds": ["https://www.novinky.cz/rss"],
        "assume_czech": False,
    },
    "Seznam Zprávy": {
        "feeds": ["https://www.seznamzpravy.cz/rss"],
        "assume_czech": False,
    },
    "iDNES.cz": {
        "feeds": ["https://servis.idnes.cz/rss.aspx?c=zpravodaj"],
        "assume_czech": False,
    },
    "Radio Prague International (EN)": {
        "feeds": ["https://english.radio.cz/rcz-rss/en"],
        "assume_czech": True,
    },
}


TOPIC_KEYWORDS = {
    "diplomacy": [
        "foreign ministry", "ministry of foreign affairs", "mfa", "diplomacy",
        "diplomatic", "talks", "negotiations", "meeting", "summit", "delegation",
        "envoy", "embassy", "ambassador", "bilateral", "multilateral", "agreement",
        "treaty", "strategic partnership", "joint statement", "consultations",
        "ministerstvo zahraničních věcí", "zahraniční ministerstvo", "diplomacie",
        "diplomat", "jednání", "vyjednávání", "schůzka", "summit", "delegace",
        "vyslanec", "velvyslanectví", "ambasáda", "velvyslanec", "bilaterální",
        "multilaterální", "dohoda", "smlouva", "strategické partnerství", "konzultace",
    ],
    "military": [
        "defense ministry", "ministry of defense", "military", "armed forces",
        "troops", "exercise", "drills", "deployment", "missile", "air defense",
        "navy", "fleet", "submarine", "weapons", "arms", "defense industry",
        "security", "nato", "ministerstvo obrany", "armáda", "ozbrojené síly",
        "voják", "vojáci", "vojensk", "cvičení", "manévry", "nasazení", "raketa",
        "protivzdušná obrana", "letectvo", "zbraně", "výzbroj", "obranný průmysl",
        "bezpečnost", "munice", "generální štáb",
    ],
    "energy": [
        "energy", "oil", "gas", "lng", "pipeline", "refinery", "electricity",
        "power grid", "nuclear power", "coal", "fuel", "energy exports", "petroleum",
        "energy security", "energetika", "energie", "ropa", "zemní plyn", "plyn",
        "lng", "ropovod", "plynovod", "rafinerie", "elektřina", "elektrárna",
        "jaderná energetika", "jaderná elektrárna", "uhlí", "palivo", "čez",
        "dukovany", "temelín", "energetická bezpečnost", "přenosová soustava",
    ],
    "economy": [
        "economy", "economic", "gdp", "inflation", "interest rate", "central bank",
        "trade", "exports", "imports", "industry", "manufacturing", "investment",
        "budget", "deficit", "banking", "sanctions", "market", "employment",
        "unemployment", "koruna", "ekonomika", "ekonomick", "hdp", "inflace",
        "úroková sazba", "úrokové sazby", "česká národní banka", "čnb", "obchod",
        "export", "vývoz", "import", "dovoz", "průmysl", "výroba", "investice",
        "rozpočet", "schodek", "deficit", "bankovnictví", "sankce", "trh",
        "zaměstnanost", "nezaměstnanost", "mzdy", "koruny", "koruně",
    ],
    "local_events": [
        "fire", "flood", "earthquake", "storm", "wildfire", "explosion", "accident",
        "crash", "evacuation", "emergency", "disaster", "landslide", "outage",
        "collapse", "rescue", "injured", "killed", "požár", "povodeň", "záplav",
        "zemětřesení", "bouře", "vichřice", "výbuch", "nehoda", "havárie",
        "evakuace", "mimořádná událost", "katastrofa", "sesuv", "výpadek",
        "zřícení", "záchrana", "zraněn", "zraněni", "zemřel", "mrtv",
    ],
}

CZECH_PATTERNS = [
    re.compile(r"\bczech(?:ia| republic| lands?| people| government| army| military| economy| president| prime minister| parliament)?\b", re.IGNORECASE),
    re.compile(r"\bprague\b", re.IGNORECASE),
    re.compile(r"\bpraha\b", re.IGNORECASE),
    re.compile(r"\bčes(?:ko|ka|ké|ký|kou|ku|kem|kých|kým|kém)\b", re.IGNORECASE),
    re.compile(r"\bčesk\w*", re.IGNORECASE),
    re.compile(r"\bčr\b", re.IGNORECASE),
]


# ============ OPTIONAL TRANSLATION ============

from deep_translator import GoogleTranslator




# ================= HELPERS =================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_space(s: str) -> str:
    return " ".join((s or "").split()).strip()


def normalize_url(url: str) -> str:
    url = normalize_space(url)
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)
    return url.rstrip("/")


def normalize_title(s: str) -> str:
    s = normalize_space((s or "").lower())
    s = re.sub(r"[^\w\s]", "", s)
    return s


def canonical_text_for_similarity(article: dict) -> str:
    return normalize_space(
        f"{article.get('title_raw', '')} {article.get('lead_raw', '')}".lower()
    )


def article_fingerprint(article: dict) -> str:
    """
    Stronger redundancy key:
    - prefer normalized URL when available
    - otherwise fall back to title + date + source
    """
    source = article.get("source", "")
    url = normalize_url(article.get("url", ""))
    title = normalize_title(article.get("title_raw", ""))
    published = article.get("published_utc", "")

    if url:
        # URL alone dedupes the same article when it appears in multiple feeds.
        return hash_key(url)

    return hash_key(source, title, published[:10])


def fetch_text(url: str) -> Optional[str]:
    sess = requests.Session()
    sess.headers.update(HEADERS)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.get(url, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and r.text:
                return r.text
            print(f"[WARN] HTTP {r.status_code} from {url}")
        except requests.RequestException as e:
            print(f"[WARN] Fetch error ({attempt}/{MAX_RETRIES}) for {url}: {e}")
        time.sleep(RETRY_SLEEP * attempt)

    return None


def _parse_entry_datetime(entry: dict) -> Optional[datetime]:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        st = entry.get(key)
        if st:
            try:
                return datetime(*st[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    for key in ("published", "updated", "created", "pubDate"):
        val = entry.get(key)
        if val:
            try:
                return pd.to_datetime(val, utc=True, errors="raise").to_pydatetime()
            except Exception:
                continue
    return None


def two_sentence_lead(text: str) -> str:
    t = normalize_space(re.sub(r"<[^>]+>", " ", text or ""))
    if not t:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", t)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}".strip()
    return t[:280].rstrip()


def translate_text(text: str) -> str:
    if not text:
        return ""

    text = normalize_space(text)

    for attempt in range(1, 4):
        try:
            translated = GoogleTranslator(
                source="auto",
                target="en"
            ).translate(text)

            if translated:
                return normalize_space(translated)

        except Exception as e:
            print(
                f"[WARN] Translation failed "
                f"({attempt}/3): {e}"
            )
            time.sleep(2 * attempt)

    print(f"[ERROR] Could not translate: {text[:100]}")
    return text


def hash_key(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()


def within_hours(dt_utc: datetime, now_utc: datetime, keep_hours: int) -> bool:
    return dt_utc >= (now_utc - timedelta(hours=keep_hours))


def mentions_czechia(text: str) -> bool:
    txt = normalize_space((text or "").lower())
    if not txt:
        return False
    return any(pattern.search(txt) for pattern in CZECH_PATTERNS)


def article_is_czech_related(article: dict) -> bool:
    # Domestic-only feeds are already scoped to Czech affairs and should not be
    # discarded merely because a headline omits the country's name.
    if article.get("assume_czech", False):
        return True

    raw_text = " ".join([
        article.get("title_raw", ""),
        article.get("lead_raw", ""),
    ])
    if mentions_czechia(raw_text):
        return True

    en_text = " ".join([
        article.get("title_en", ""),
        article.get("lead_en", ""),
    ])
    return mentions_czechia(en_text)


def classify_topics(article: dict) -> dict:
    txt = " ".join([
        article.get("title_raw", ""), article.get("lead_raw", ""),
        article.get("title_en", ""), article.get("lead_en", ""),
    ]).lower()
    scores = {}

    for topic, keywords in TOPIC_KEYWORDS.items():
        scores[topic] = sum(1 for kw in keywords if kw in txt)

    labels = []
    if scores["diplomacy"] >= 1:
        labels.append("diplomacy")
    if scores["military"] >= 1:
        labels.append("military")
    if scores["energy"] >= 1:
        labels.append("energy")
    if scores["economy"] >= 2:
        labels.append("economy")
    if scores["local_events"] >= 2:
        labels.append("local_events")

    primary = None
    if any(v > 0 for v in scores.values()):
        primary = max(scores, key=scores.get)

    article["topic_scores"] = scores
    article["topics"] = labels
    article["primary_topic"] = primary
    return article


# ================= RSS COLLECTOR =================

def collect_from_rss(source_name: str, feeds: List[str], assume_czech: bool = False) -> List[dict]:
    rows: List[dict] = []

    for feed_url in feeds:
        txt = fetch_text(feed_url)
        if not txt:
            continue

        d = feedparser.parse(txt)
        for e in d.entries:
            title = normalize_space(e.get("title", ""))
            url = normalize_url(e.get("link", ""))
            summary = e.get("summary", "") or e.get("description", "") or ""
            dt = _parse_entry_datetime(e)
            if not dt:
                continue

            rows.append({
                "source": source_name,
                "assume_czech": assume_czech,
                "url": url,
                "published_utc": dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title_raw": title,
                "lead_raw": two_sentence_lead(summary),
            })

        if rows:
            break

    return rows


# ================= STORE LOGIC =================

def load_existing_json(path: Path) -> List[dict]:
    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("articles"), list):
            return data["articles"]
        if isinstance(data, list):
            return data
    except Exception as e:
        print(f"[WARN] Failed loading existing JSON from {path}: {e}")

    return []


def choose_better_article(a: dict, b: dict) -> dict:
    """
    Keep the better/more complete version when duplicates collide.
    """
    score_a = 0
    score_b = 0

    for field in ("title_raw", "lead_raw", "title_en", "lead_en", "url"):
        score_a += len(a.get(field, "") or "")
        score_b += len(b.get(field, "") or "")

    # Slight preference for newer copy when completeness is tied
    if score_b > score_a:
        return b
    return a


def dedupe_articles(rows: List[dict]) -> List[dict]:
    best: Dict[str, dict] = {}

    for r in rows:
        key = article_fingerprint(r)
        prev = best.get(key)
        if prev is None:
            best[key] = r
        else:
            best[key] = choose_better_article(prev, r)

    final = list(best.values())
    final.sort(key=lambda x: x.get("published_utc", ""), reverse=True)
    return final


def prune_latest_window(rows: List[dict]) -> List[dict]:
    now = _utcnow()
    kept: List[dict] = []

    for r in rows:
        try:
            dt = pd.to_datetime(r.get("published_utc"), utc=True, errors="raise").to_pydatetime()
        except Exception:
            continue
        if within_hours(dt, now, KEEP_HOURS):
            kept.append(r)

    return dedupe_articles(kept)


def enrich_translate(rows: List[dict]) -> List[dict]:
    out: List[dict] = []

    for r in rows:
        title_raw = r.get("title_raw", "")
        lead_raw = r.get("lead_raw", "")

        title_en = translate_text(title_raw)
        lead_en = translate_text(lead_raw)

        item = {
            "id": hash_key(
                r.get("source", ""),
                normalize_url(r.get("url", "")),
                r.get("published_utc", ""),
                normalize_title(title_raw),
            ),
            "fingerprint": article_fingerprint(r),
            "source": r.get("source", ""),
            "url": normalize_url(r.get("url", "")),
            "published_utc": r.get("published_utc", ""),
            "title_en": title_en,
            "lead_en": two_sentence_lead(lead_en),
            "title_raw": title_raw,
            "lead_raw": lead_raw,
        }

        out.append(item)

    return out


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ================= MAIN =================

def main() -> int:
    now = _utcnow()
    print(f"[INFO] Run at {now.strftime('%Y-%m-%dT%H:%M:%SZ')} | keep_hours={KEEP_HOURS}")

    collected: List[dict] = []
    for name, cfg in SOURCES.items():
        print(f"[INFO] RSS: {name}")
        rows = collect_from_rss(name, cfg["feeds"], cfg.get("assume_czech", False))
        print(f"[INFO] {name}: {len(rows)} rows")
        collected.extend(rows)

    if not collected:
        print("[WARN] No articles collected.")
        return 0

    # Load prior rolling and archive data
    # Drop records from a prior non-Czech version of the tracker while keeping
    # the same output filenames for downstream compatibility.
    existing_latest = [
        r for r in load_existing_json(OUT_JSON)
        if r.get("tracker_country") == "CZ"
    ]
    existing_archive = [
        r for r in load_existing_json(ARCHIVE_JSON)
        if r.get("tracker_country") == "CZ"
    ]

    # Merge raw records first
    merged_raw_for_latest = existing_latest + collected
    merged_raw_for_archive = existing_archive + collected

    # Keep rolling latest window
    latest_raw = prune_latest_window(merged_raw_for_latest)

    # Keep persistent archive with dedupe only
    archive_raw = dedupe_articles(merged_raw_for_archive)

    # Enrich/translate
    latest_enriched = enrich_translate(latest_raw)
    archive_enriched = enrich_translate(archive_raw)

    # Czech relevance filter
    latest_filtered = [a for a in latest_enriched if article_is_czech_related(a)]
    archive_filtered = [a for a in archive_enriched if article_is_czech_related(a)]

    print(f"[INFO] Latest Czech-related: {len(latest_filtered)} / {len(latest_enriched)} kept")
    print(f"[INFO] Archive Czech-related: {len(archive_filtered)} / {len(archive_enriched)} kept")

    latest_classified = [classify_topics(a) for a in latest_filtered]
    archive_classified = [classify_topics(a) for a in archive_filtered]

    latest_payload = {
        "country": "Czech Republic",
        "country_code": "CZ",
        "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "keep_hours": KEEP_HOURS,
        "count": len(latest_classified),
        "articles": latest_classified,
    }
    atomic_write_json(OUT_JSON, latest_payload)

    archive_payload = {
        "country": "Czech Republic",
        "country_code": "CZ",
        "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(archive_classified),
        "articles": archive_classified,
    }
    atomic_write_json(ARCHIVE_JSON, archive_payload)

    topic_names = ["diplomacy", "military", "energy", "economy", "local_events"]
    for topic in topic_names:
        subset = [a for a in latest_classified if topic in a.get("topics", [])]
        topic_payload = {
            "country": "Czech Republic",
            "country_code": "CZ",
            "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "topic": topic,
            "count": len(subset),
            "articles": subset,
        }
        atomic_write_json(Path(f"data/articles_{topic}.json"), topic_payload)

    print(f"[OK] Wrote latest rolling file -> {OUT_JSON}")
    print(f"[OK] Wrote persistent archive -> {ARCHIVE_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
