# -*- coding: utf-8 -*-
"""
Czech Media Tracker — Collector + Translator + Topic Splitter

- Collects Czech news from RSS feeds
- Translates new articles using GoogleTranslator
- Never retranslates successfully archived articles
- Maintains a persistent archive
- Generates a rolling 48-hour news feed
- Generates individual topic files
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

from deep_translator import GoogleTranslator
from deep_translator.exceptions import TooManyRequests


# ================= CONFIG =================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,cs;q=0.8",
    "Connection": "keep-alive",
}

TIMEOUT = 20
MAX_RETRIES = 3
RETRY_SLEEP = 2.0

KEEP_HOURS = int(os.getenv("KEEP_HOURS", "48"))

TRANSLATION_DELAY = float(
    os.getenv("TRANSLATION_DELAY", "1.5")
)

OUT_JSON = Path(
    os.getenv("OUT_JSON", "data/articles_latest.json")
)

ARCHIVE_JSON = Path(
    os.getenv("ARCHIVE_JSON", "data/articles_archive.json")
)

translator = GoogleTranslator(
    source="cs",
    target="en"
)


# ================= SOURCES =================

SOURCES: Dict[str, Dict] = {

    "iROZHLAS - Domestic": {
        "feeds": [
            "https://www.irozhlas.cz/rss/irozhlas/section/zpravy-domov",
        ],
    },

    "iROZHLAS - Economy": {
        "feeds": [
            "https://www.irozhlas.cz/rss/irozhlas/section/ekonomika",
        ],
    },

    "České noviny / ČTK - Czech Republic": {
        "feeds": [
            "https://www.ceskenoviny.cz/sluzby/rss/cr.php",
        ],
    },

    "České noviny / ČTK - Economy": {
        "feeds": [
            "https://www.ceskenoviny.cz/sluzby/rss/ekonomika.php",
        ],
    },

    "ČT24": {
        "feeds": [
            "https://ct24.ceskatelevize.cz/rss",
        ],
    },

    "Novinky": {
        "feeds": [
            "https://www.novinky.cz/rss",
        ],
    },

    "Seznam Zprávy": {
        "feeds": [
            "https://www.seznamzpravy.cz/rss",
        ],
    },

    "iDNES.cz": {
        "feeds": [
            "https://servis.idnes.cz/rss.aspx?c=zpravodaj",
        ],
    },

    "Radio Prague International (EN)": {
        "feeds": [
            "https://english.radio.cz/rcz-rss/en",
        ],
    },
}


# ================= TOPIC KEYWORDS =================

TOPIC_KEYWORDS = {

    "diplomacy": [

        # English
        "foreign ministry",
        "ministry of foreign affairs",
        "diplomacy",
        "diplomatic",
        "talks",
        "negotiations",
        "meeting",
        "summit",
        "delegation",
        "envoy",
        "embassy",
        "ambassador",
        "bilateral",
        "multilateral",
        "agreement",
        "treaty",
        "strategic partnership",
        "joint statement",
        "consultations",
        "foreign policy",
        "state visit",

        # Czech
        "ministerstvo zahraničních věcí",
        "zahraniční politika",
        "diplomac",
        "diplomat",
        "jednán",
        "vyjednáv",
        "schůzk",
        "summit",
        "delegac",
        "velvyslan",
        "ambasád",
        "vyslanec",
        "bilaterál",
        "multilaterál",
        "dohod",
        "smlouv",
        "strategické partnerství",
        "konzultac",
        "státní návštěv",
    ],

    "military": [

        # English
        "defense ministry",
        "ministry of defense",
        "military",
        "armed forces",
        "troops",
        "exercise",
        "drills",
        "deployment",
        "missile",
        "air defense",
        "navy",
        "fleet",
        "submarine",
        "weapons",
        "arms",
        "defense industry",
        "security",
        "nato",
        "intelligence",
        "cyber attack",
        "hybrid warfare",

        # Czech
        "ministerstvo obrany",
        "armáda",
        "ozbrojené síly",
        "voják",
        "vojensk",
        "cvičen",
        "manévr",
        "nasazen",
        "raket",
        "protivzduš",
        "letectv",
        "zbran",
        "výzbroj",
        "obrann",
        "zbroj",
        "munic",
        "bezpečnost",
        "nato",
        "generální štáb",
        "bis",
        "vojenské zpravodajství",
        "kybernetick",
        "hybridní",
    ],

    "energy": [

        # English
        "energy",
        "oil",
        "gas",
        "lng",
        "pipeline",
        "refinery",
        "electricity",
        "power grid",
        "nuclear power",
        "coal",
        "fuel",
        "energy exports",
        "energy security",
        "petroleum",
        "nuclear",
        "power plant",

        # Czech
        "energet",
        "energie",
        "ropa",
        "plyn",
        "lng",
        "ropovod",
        "plynovod",
        "rafineri",
        "elektřin",
        "elektrárn",
        "jadern",
        "uhlí",
        "paliv",
        "čez",
        "dukovany",
        "temelín",
        "teplár",
        "energetická bezpečnost",
        "přenosová soustava",
        "emisní povolen",
    ],

    "economy": [

        # English
        "economy",
        "economic",
        "gdp",
        "inflation",
        "interest rate",
        "central bank",
        "trade",
        "exports",
        "imports",
        "industry",
        "manufacturing",
        "investment",
        "budget",
        "deficit",
        "banking",
        "sanctions",
        "market",
        "employment",
        "unemployment",
        "wages",
        "tax",

        # Czech
        "ekonom",
        "hdp",
        "inflac",
        "úrok",
        "čnb",
        "česká národní banka",
        "korun",
        "obchod",
        "export",
        "vývoz",
        "import",
        "dovoz",
        "průmysl",
        "výrob",
        "investic",
        "rozpočet",
        "schodek",
        "deficit",
        "bank",
        "sankc",
        "trh",
        "zaměstnan",
        "nezaměstnan",
        "mzd",
        "daň",
    ],

    "local_events": [

        # English
        "fire",
        "flood",
        "earthquake",
        "storm",
        "wildfire",
        "explosion",
        "accident",
        "crash",
        "evacuation",
        "emergency",
        "disaster",
        "landslide",
        "outage",
        "collapse",
        "rescue",
        "injured",
        "killed",

        # Czech
        "požár",
        "povodeň",
        "záplav",
        "zemětřesení",
        "bouř",
        "vichřic",
        "výbuch",
        "nehod",
        "havári",
        "evakuac",
        "mimořádná událost",
        "katastrof",
        "sesuv",
        "výpadek",
        "zřícen",
        "záchran",
        "zraněn",
        "zemřel",
        "mrtv",
    ],
}


# ================= GENERAL HELPERS =================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_space(s: str) -> str:
    return " ".join((s or "").split()).strip()


def normalize_url(url: str) -> str:

    url = normalize_space(url)

    # Remove URL fragments and tracking parameters
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)

    return url.rstrip("/")


def normalize_title(s: str) -> str:

    s = normalize_space((s or "").lower())
    s = re.sub(r"[^\w\s]", "", s)

    return s


def hash_key(*parts: str) -> str:

    h = hashlib.sha1()

    for p in parts:
        h.update(
            (p or "").encode(
                "utf-8",
                errors="ignore"
            )
        )

        h.update(b"|")

    return h.hexdigest()


def article_fingerprint(article: dict) -> str:
    """
    Generate a stable identifier for an article.

    Prefer the source and normalized URL.
    If no URL exists, use source, title, and date.
    """

    source = article.get("source", "")

    url = normalize_url(
        article.get("url", "")
    )

    title = normalize_title(
        article.get("title_raw", "")
    )

    published = article.get("published_utc", "")

    if url:
        return hash_key(source, url)

    return hash_key(
        source,
        title,
        published[:10]
    )


def within_hours(
    dt_utc: datetime,
    now_utc: datetime,
    keep_hours: int
) -> bool:

    return dt_utc >= (
        now_utc - timedelta(hours=keep_hours)
    )


# ================= RSS FETCHING =================

def fetch_text(url: str) -> Optional[str]:

    sess = requests.Session()
    sess.headers.update(HEADERS)

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            r = sess.get(
                url,
                timeout=TIMEOUT,
                allow_redirects=True
            )

            if r.status_code == 200 and r.text:
                return r.text

            print(
                f"[WARN] HTTP {r.status_code} from {url}"
            )

        except requests.RequestException as e:

            print(
                f"[WARN] Fetch error "
                f"({attempt}/{MAX_RETRIES}) "
                f"for {url}: {e}"
            )

        time.sleep(RETRY_SLEEP * attempt)

    return None


def _parse_entry_datetime(
    entry: dict
) -> Optional[datetime]:

    for key in (
        "published_parsed",
        "updated_parsed",
        "created_parsed"
    ):

        st = entry.get(key)

        if st:

            try:

                return datetime(
                    *st[:6],
                    tzinfo=timezone.utc
                )

            except Exception:
                pass

    for key in (
        "published",
        "updated",
        "created",
        "pubDate"
    ):

        val = entry.get(key)

        if val:

            try:

                return pd.to_datetime(
                    val,
                    utc=True,
                    errors="raise"
                ).to_pydatetime()

            except Exception:
                continue

    return None


def two_sentence_lead(text: str) -> str:
    """
    Remove HTML and retain the first two sentences.

    If sentence boundaries cannot be identified,
    retain up to 280 characters.
    """

    t = normalize_space(
        re.sub(
            r"<[^>]+>",
            " ",
            text or ""
        )
    )

    if not t:
        return ""

    parts = re.split(
        r"(?<=[.!?])\s+",
        t
    )

    parts = [
        p.strip()
        for p in parts
        if p.strip()
    ]

    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}".strip()

    return t[:280].rstrip()


def collect_from_rss(
    source_name: str,
    feeds: List[str]
) -> List[dict]:

    rows: List[dict] = []

    for feed_url in feeds:

        txt = fetch_text(feed_url)

        if not txt:
            continue

        d = feedparser.parse(txt)

        for e in d.entries:

            title = normalize_space(
                e.get("title", "")
            )

            url = normalize_url(
                e.get("link", "")
            )

            summary = (
                e.get("summary", "")
                or e.get("description", "")
                or ""
            )

            dt = _parse_entry_datetime(e)

            if not dt:
                continue

            rows.append({

                "source": source_name,

                "url": url,

                "published_utc": dt.astimezone(
                    timezone.utc
                ).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),

                "title_raw": title,

                "lead_raw": two_sentence_lead(
                    summary
                ),
            })

        if rows:
            break

    return rows


# ================= TRANSLATION =================

def translate_text(text: str) -> str:
    """
    Translate Czech text into English.

    Includes request spacing and retries.

    Translation failures are not silently saved
    as successful English translations.
    """

    if not text:
        return ""

    for attempt in range(MAX_RETRIES):

        # Space requests to reduce rate limiting
        time.sleep(TRANSLATION_DELAY)

        try:

            result = translator.translate(text)

            if not result or not result.strip():
                raise ValueError("Empty translation")

            result = result.strip()

            print(
                f"[TRANSLATE] "
                f"{text[:60]} -> {result[:60]}"
            )

            return result

        except TooManyRequests:

            # Stop the current run rather than repeatedly
            # hitting Google's rate-limited endpoint.
            raise

        except Exception as exc:

            print(
                f"[WARN] Translation attempt "
                f"{attempt + 1}/{MAX_RETRIES}: {exc}"
            )

            if attempt == MAX_RETRIES - 1:
                raise

            # Increasing retry delay
            time.sleep(5 * (attempt + 1))


# ================= TOPIC CLASSIFICATION =================

def classify_topics(article: dict) -> dict:

    # Search original Czech and translated English text
    txt = (
        f"{article.get('title_raw', '')} "
        f"{article.get('lead_raw', '')} "
        f"{article.get('title_en', '')} "
        f"{article.get('lead_en', '')}"
    ).lower()

    scores = {}

    for topic, keywords in TOPIC_KEYWORDS.items():

        scores[topic] = sum(
            1
            for kw in keywords
            if kw.lower() in txt
        )

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

        primary = max(
            scores,
            key=scores.get
        )

    article["topic_scores"] = scores
    article["topics"] = labels
    article["primary_topic"] = primary

    return article


# ================= ARTICLE ENRICHMENT =================

def enrich_article(row: dict) -> dict:
    """
    Translate and classify one new article.

    Articles from Radio Prague International
    are already in English and bypass translation.
    """

    english = (
        row["source"] == "Radio Prague International (EN)"
    )

    title = row["title_raw"]
    lead = row["lead_raw"]

    if english:

        title_en = title
        lead_en = lead

    else:

        title_en = translate_text(title)
        lead_en = translate_text(lead)

    item = {

        **row,

        "id": hash_key(
            row["source"],
            normalize_url(row["url"]),
            row["published_utc"],
            normalize_title(title)
        ),

        "fingerprint": article_fingerprint(row),

        "title_en": title_en,

        "lead_en": lead_en,
    }

    return classify_topics(item)


# ================= ARCHIVE MANAGEMENT =================

def load_archive() -> dict:
    """
    Load the persistent archive as a dictionary.

    The archive is the permanent translation cache.
    Any article already present is skipped.
    """

    if not ARCHIVE_JSON.exists():
        return {}

    # Fail explicitly if the archive is corrupted.
    # This prevents accidental loss of translation history.

    data = json.loads(
        ARCHIVE_JSON.read_text(
            encoding="utf-8"
        )
    )

    rows = data["articles"]

    if not isinstance(rows, list):
        raise ValueError("Invalid archive format")

    return {
        article_fingerprint(a): a
        for a in rows
    }


def sorted_articles(archive: dict) -> list:

    return sorted(
        archive.values(),
        key=lambda a: a["published_utc"],
        reverse=True
    )


# ================= JSON OUTPUT =================

def atomic_write_json(
    path: Path,
    payload: dict
) -> None:
    """
    Write JSON through a temporary file to reduce
    the risk of corruption during interrupted writes.
    """

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    tmp = path.with_suffix(".tmp")

    tmp.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )

    tmp.replace(path)


def payload(
    rows: list,
    now: datetime,
    **extra
) -> dict:

    return {

        "country": "Czech Republic",

        "country_code": "CZ",

        "updated_at_utc": now.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),

        **extra,

        "count": len(rows),

        "articles": rows,
    }


# ================= MAIN =================

def main() -> int:

    now = _utcnow()

    print("=" * 60)
    print("[INFO] Czech Media Tracker")
    print(f"[INFO] Run time: {now.isoformat()}")
    print(f"[INFO] Rolling window: {KEEP_HOURS} hours")
    print(f"[INFO] Translation delay: {TRANSLATION_DELAY}s")
    print("=" * 60)

    # ---------------------------------------------
    # 1. Load existing translation archive
    # ---------------------------------------------

    archive = load_archive()

    print(
        f"[INFO] {len(archive)} previously translated articles"
    )

    candidates = {}

    # ---------------------------------------------
    # 2. Collect RSS articles
    # ---------------------------------------------

    for name, cfg in SOURCES.items():

        rows = collect_from_rss(
            name,
            cfg["feeds"]
        )

        print(
            f"[INFO] {name}: {len(rows)} RSS entries"
        )

        for row in rows:

            dt = datetime.fromisoformat(
                row["published_utc"].replace(
                    "Z",
                    "+00:00"
                )
            )

            # Ignore articles without titles
            if not row["title_raw"]:
                continue

            # Ignore articles outside rolling window
            if not within_hours(
                dt,
                now,
                KEEP_HOURS
            ):
                continue

            key = article_fingerprint(row)

            # Only retain articles not already archived
            if key not in archive:

                candidates.setdefault(
                    key,
                    row
                )

    print(
        f"[INFO] {len(candidates)} new articles to process"
    )

    # ---------------------------------------------
    # 3. Translate only previously unseen articles
    # ---------------------------------------------

    added = 0
    failed = 0

    for key, row in candidates.items():

        try:

            item = enrich_article(row)

        except TooManyRequests:

            print(
                "[WARN] Google rate limit reached."
            )

            print(
                "[WARN] Stopping translations for this run."
            )

            print(
                "[WARN] Successfully translated articles "
                "will still be saved."
            )

            failed += 1

            break

        except Exception as exc:

            print(
                f"[WARN] Skipping untranslated article "
                f"{row['url']}: {exc}"
            )

            failed += 1

            continue

        # Add successfully processed article to archive
        archive[key] = item

        added += 1

        print(
            f"[INFO] Added article "
            f"{added}/{len(candidates)}"
        )

        # Save progress after every successful article
        atomic_write_json(
            ARCHIVE_JSON,
            payload(
                sorted_articles(archive),
                now
            )
        )

    # ---------------------------------------------
    # 4. Generate persistent archive
    # ---------------------------------------------

    all_articles = sorted_articles(archive)

    atomic_write_json(
        ARCHIVE_JSON,
        payload(
            all_articles,
            now
        )
    )

    # ---------------------------------------------
    # 5. Generate rolling 48-hour feed
    # ---------------------------------------------

    latest = [

        a for a in all_articles

        if within_hours(

            datetime.fromisoformat(
                a["published_utc"].replace(
                    "Z",
                    "+00:00"
                )
            ),

            now,

            KEEP_HOURS
        )
    ]

    atomic_write_json(

        OUT_JSON,

        payload(
            latest,
            now,
            keep_hours=KEEP_HOURS
        )
    )

    # ---------------------------------------------
    # 6. Generate individual topic files
    # ---------------------------------------------

    for topic in TOPIC_KEYWORDS:

        subset = [

            a for a in latest

            if topic in a.get("topics", [])
        ]

        atomic_write_json(

            Path(
                f"data/articles_{topic}.json"
            ),

            payload(
                subset,
                now,
                topic=topic
            )
        )

        print(
            f"[OK] {topic}: {len(subset)} articles"
        )

    # ---------------------------------------------
    # 7. Print execution summary
    # ---------------------------------------------

    print("=" * 60)

    print("[OK] Czech Media Tracker completed")

    print(
        f"[OK] New articles added: {added}"
    )

    print(
        f"[INFO] New articles not added: "
        f"{len(candidates) - added}"
    )

    print(
        f"[INFO] Translation failures: {failed}"
    )

    print(
        f"[OK] Latest articles: {len(latest)}"
    )

    print(
        f"[OK] Total archived articles: {len(all_articles)}"
    )

    print(
        f"[OK] Latest file: {OUT_JSON}"
    )

    print(
        f"[OK] Archive file: {ARCHIVE_JSON}"
    )

    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
