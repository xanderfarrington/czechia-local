# -*- coding: utf-8 -*-
"""
Czech Media Tracker

RSS collection, topic classification, and Czech-to-English
translation using NLLB-200 distilled 1.3B.

Features:
- Persistent article archive
- Rolling 48-hour news window
- Pre-translation topic classification
- Batched local translation
- Pending translation queue
- No retranslations of completed articles
- Independent processing of English-language sources
"""

from __future__ import annotations

import os
import re
import json
import time
import html
import hashlib

from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
import feedparser
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

KEEP_HOURS = int(
    os.getenv("KEEP_HOURS", "48")
)

TRANSLATE_LIMIT = int(
    os.getenv("TRANSLATE_LIMIT", "60")
)

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "4")
)

NUM_BEAMS = int(
    os.getenv("NUM_BEAMS", "4")
)

CPU_THREADS = int(
    os.getenv("CPU_THREADS", "4")
)

MODEL_NAME = os.getenv(
    "MODEL_NAME",
    "facebook/nllb-200-distilled-1.3B"
)

TRANSLATE_ALL = (
    os.getenv("TRANSLATE_ALL", "0") == "1"
)

SOURCE_LANGUAGE = "ces_Latn"
TARGET_LANGUAGE = "eng_Latn"

TIMEOUT = 20
MAX_RETRIES = 3
RETRY_SLEEP = 2

MAX_INPUT_TOKENS = 512
MAX_NEW_TOKENS = 256

OUT_JSON = Path(
    os.getenv(
        "OUT_JSON",
        "data/articles_latest.json"
    )
)

ARCHIVE_JSON = Path(
    os.getenv(
        "ARCHIVE_JSON",
        "data/articles_archive.json"
    )
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "application/rss+xml, application/xml, "
        "text/xml, */*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9,cs;q=0.8",
    "Connection": "keep-alive",
}

# Translation model is loaded only when needed.

TOKENIZER = None
MODEL = None
TORCH = None


# ============================================================
# RSS SOURCES
# ============================================================

SOURCES = {

    "iROZHLAS - Domestic": {
        "language": "cs",
        "feeds": [
            "https://www.irozhlas.cz/rss/irozhlas/section/zpravy-domov"
        ],
    },

    "iROZHLAS - Economy": {
        "language": "cs",
        "feeds": [
            "https://www.irozhlas.cz/rss/irozhlas/section/ekonomika"
        ],
    },

    "České noviny / ČTK - Czech Republic": {
        "language": "cs",
        "feeds": [
            "https://www.ceskenoviny.cz/sluzby/rss/cr.php"
        ],
    },

    "České noviny / ČTK - Economy": {
        "language": "cs",
        "feeds": [
            "https://www.ceskenoviny.cz/sluzby/rss/ekonomika.php"
        ],
    },

    "ČT24": {
        "language": "cs",
        "feeds": [
            "https://ct24.ceskatelevize.cz/rss"
        ],
    },

    "Novinky": {
        "language": "cs",
        "feeds": [
            "https://www.novinky.cz/rss"
        ],
    },

    "Seznam Zprávy": {
        "language": "cs",
        "feeds": [
            "https://www.seznamzpravy.cz/rss"
        ],
    },

    "iDNES.cz": {
        "language": "cs",
        "feeds": [
            "https://servis.idnes.cz/rss.aspx?c=zpravodaj"
        ],
    },

    "Radio Prague International (EN)": {
        "language": "en",
        "feeds": [
            "https://english.radio.cz/rcz-rss/en"
        ],
    },
}


# ============================================================
# TOPIC KEYWORDS
# ============================================================

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
        "zemětřesen",
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


TOPIC_THRESHOLDS = {
    "diplomacy": 1,
    "military": 1,
    "energy": 1,
    "economy": 2,
    "local_events": 2,
}


# ============================================================
# GENERAL HELPERS
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utc_string(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def normalize_space(text: str) -> str:

    return " ".join(
        (text or "").split()
    ).strip()


def clean_text(text: str) -> str:

    text = html.unescape(text or "")

    text = re.sub(
        r"<[^>]+>",
        " ",
        text
    )

    return normalize_space(text)


def normalize_url(url: str) -> str:

    url = normalize_space(url)

    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)

    return url.rstrip("/")


def normalize_title(text: str) -> str:

    text = normalize_space(
        (text or "").lower()
    )

    return re.sub(
        r"[^\w\s]",
        "",
        text
    )


def hash_key(*parts: str) -> str:

    h = hashlib.sha1()

    for part in parts:

        h.update(
            (part or "").encode("utf-8")
        )

        h.update(b"|")

    return h.hexdigest()


def article_fingerprint(article: dict) -> str:

    source = article.get("source", "")

    url = normalize_url(
        article.get("url", "")
    )

    title = normalize_title(
        article.get("title_raw", "")
    )

    published = article.get(
        "published_utc",
        ""
    )

    if url:
        return hash_key(source, url)

    return hash_key(
        source,
        title,
        published[:10]
    )


def within_window(
    article: dict,
    now: datetime
) -> bool:

    try:

        published = datetime.fromisoformat(
            article["published_utc"].replace(
                "Z",
                "+00:00"
            )
        )

        return (
            now - timedelta(hours=KEEP_HOURS)
            <= published
            <= now + timedelta(minutes=10)
        )

    except (ValueError, KeyError, TypeError):

        return False


def two_sentence_lead(text: str) -> str:

    text = clean_text(text)

    if not text:
        return ""

    sentences = re.split(
        r"(?<=[.!?])\s+",
        text
    )

    sentences = [
        sentence.strip()
        for sentence in sentences
        if sentence.strip()
    ]

    if len(sentences) >= 2:

        return " ".join(
            sentences[:2]
        )

    return text[:280].rstrip()


# ============================================================
# RSS COLLECTION
# ============================================================

def fetch_text(url: str) -> Optional[str]:

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT,
                allow_redirects=True
            )

            if (
                response.status_code == 200
                and response.text
            ):

                return response.text

            print(
                f"[WARN] HTTP "
                f"{response.status_code}: {url}"
            )

        except requests.RequestException as exc:

            print(
                f"[WARN] Fetch attempt "
                f"{attempt}/{MAX_RETRIES}: {exc}"
            )

        if attempt < MAX_RETRIES:

            time.sleep(
                RETRY_SLEEP * attempt
            )

    return None


def parse_entry_datetime(
    entry: dict
) -> Optional[datetime]:

    for key in (
        "published_parsed",
        "updated_parsed",
        "created_parsed"
    ):

        value = entry.get(key)

        if value:

            try:

                return datetime(
                    *value[:6],
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

        value = entry.get(key)

        if value:

            try:

                return pd.to_datetime(
                    value,
                    utc=True,
                    errors="raise"
                ).to_pydatetime()

            except Exception:

                pass

    return None


def collect_from_rss(
    source_name: str,
    config: dict,
    now: datetime
) -> List[dict]:

    rows = []

    for feed_url in config["feeds"]:

        raw_feed = fetch_text(feed_url)

        if not raw_feed:
            continue

        feed = feedparser.parse(raw_feed)

        print(
            f"[INFO] {source_name}: "
            f"{len(feed.entries)} RSS entries"
        )

        for entry in feed.entries:

            title = clean_text(
                entry.get("title", "")
            )

            if not title:
                continue

            published = parse_entry_datetime(
                entry
            )

            if published is None:
                continue

            if not (
                now - timedelta(hours=KEEP_HOURS)
                <= published
                <= now + timedelta(minutes=10)
            ):
                continue

            url = normalize_url(
                entry.get("link", "")
            )

            summary = (
                entry.get("summary", "")
                or entry.get("description", "")
                or ""
            )

            row = {

                "source": source_name,

                "source_language": config["language"],

                "url": url,

                "published_utc": utc_string(
                    published
                ),

                "title_raw": title,

                "lead_raw": two_sentence_lead(
                    summary
                ),
            }

            rows.append(row)

        if rows:
            break

    return rows


# ============================================================
# TOPIC CLASSIFICATION
# ============================================================

def classify_topics(
    article: dict,
    include_translation: bool = True
) -> dict:

    text = (
        f"{article.get('title_raw') or ''} "
        f"{article.get('lead_raw') or ''}"
    )

    if include_translation:

        text += (
            f" {article.get('title_en') or ''}"
            f" {article.get('lead_en') or ''}"
        )

    text = text.lower()

    scores = {}

    for topic, keywords in TOPIC_KEYWORDS.items():

        # Avoid counting identical keywords twice.
        unique_keywords = set(
            keyword.lower()
            for keyword in keywords
        )

        scores[topic] = sum(

            1

            for keyword in unique_keywords

            if keyword in text
        )

    labels = [

        topic

        for topic, score in scores.items()

        if score >= TOPIC_THRESHOLDS[topic]
    ]

    primary_topic = (

        max(
            labels,
            key=lambda topic: scores[topic]
        )

        if labels

        else None
    )

    article["topic_scores"] = scores
    article["topics"] = labels
    article["primary_topic"] = primary_topic
    article["relevant"] = bool(labels)

    return article


# ============================================================
# NLLB TRANSLATION MODEL
# ============================================================

def load_translator() -> None:

    global TOKENIZER
    global MODEL
    global TORCH

    if MODEL is not None:
        return

    print(
        f"[MODEL] Loading {MODEL_NAME}"
    )

    start = time.monotonic()

    import torch

    from transformers import (
        AutoTokenizer,
        AutoModelForSeq2SeqLM
    )

    TORCH = torch

    torch.set_num_threads(CPU_THREADS)

    TOKENIZER = AutoTokenizer.from_pretrained(

        MODEL_NAME,

        src_lang=SOURCE_LANGUAGE,

        tgt_lang=TARGET_LANGUAGE,
    )

    MODEL = AutoModelForSeq2SeqLM.from_pretrained(

        MODEL_NAME,

        torch_dtype=torch.float32,

        low_cpu_mem_usage=True,
    )

    MODEL.to("cpu")
    MODEL.eval()

    elapsed = time.monotonic() - start

    print(
        f"[MODEL] Loaded in {elapsed:.1f}s"
    )

    print(
        f"[MODEL] Source language: "
        f"{SOURCE_LANGUAGE}"
    )

    print(
        f"[MODEL] Target language: "
        f"{TARGET_LANGUAGE}"
    )

    print(
        f"[MODEL] CPU threads: {CPU_THREADS}"
    )

    print(
        f"[MODEL] Beam search: {NUM_BEAMS}"
    )


def translate_batch(
    texts: List[str]
) -> List[str]:

    if not texts:
        return []

    load_translator()

    texts = [
        normalize_space(text)
        for text in texts
    ]

    results = [""] * len(texts)

    # Empty summaries should not consume inference time.

    nonempty = [

        (index, text)

        for index, text in enumerate(texts)

        if text
    ]

    if not nonempty:
        return results

    input_texts = [

        text

        for _, text in nonempty
    ]

    inputs = TOKENIZER(

        input_texts,

        return_tensors="pt",

        padding=True,

        truncation=True,

        max_length=MAX_INPUT_TOKENS,
    )

    target_token = TOKENIZER.convert_tokens_to_ids(
        TARGET_LANGUAGE
    )

    if (
        target_token is None
        or target_token == TOKENIZER.unk_token_id
    ):
        raise ValueError(
            "Invalid NLLB target language token"
        )

    with TORCH.inference_mode():

        outputs = MODEL.generate(

            **inputs,

            forced_bos_token_id=target_token,

            max_new_tokens=MAX_NEW_TOKENS,

            num_beams=NUM_BEAMS,

            do_sample=False,

            early_stopping=(NUM_BEAMS > 1),
        )

    decoded = TOKENIZER.batch_decode(

        outputs,

        skip_special_tokens=True,
    )

    if len(decoded) != len(nonempty):

        raise ValueError(
            "Unexpected number of translations"
        )

    for (index, original), translated in zip(
        nonempty,
        decoded
    ):

        translated = normalize_space(
            translated
        )

        if not translated:

            raise ValueError(
                f"Empty translation: {original[:80]}"
            )

        results[index] = translated

    return results


# ============================================================
# ARTICLE PREPARATION
# ============================================================

def prepare_article(
    row: dict
) -> dict:

    key = article_fingerprint(row)

    item = {

        **row,

        "id": hash_key(

            row.get("source", ""),

            row.get("url", ""),

            row.get("published_utc", ""),

            normalize_title(
                row.get("title_raw", "")
            )
        ),

        "fingerprint": key,

        "title_en": None,

        "lead_en": None,

        "translation_status": "pending",

        "translation_attempts": 0,

        "translation_provider": None,

        "translation_error": None,

        "translated_at_utc": None,

        "collected_at_utc": utc_string(
            utcnow()
        ),
    }

    # English sources never enter the translation queue.

    if row["source_language"] == "en":

        item["title_en"] = row["title_raw"]
        item["lead_en"] = row["lead_raw"]

        item["translation_status"] = (
            "not_required"
        )

        item["translation_provider"] = (
            "original_english"
        )

    # Classify before translation.

    classify_topics(
        item,
        include_translation=False
    )

    if (
        item["source_language"] == "cs"
        and not item["relevant"]
        and not TRANSLATE_ALL
    ):

        item["translation_status"] = (
            "not_relevant"
        )

    return item


def needs_translation(
    article: dict
) -> bool:

    return (

        article.get("source_language") == "cs"

        and article.get("translation_status") in (
            "pending",
            "failed"
        )
    )


# ============================================================
# ARCHIVE MANAGEMENT
# ============================================================

def load_archive() -> Dict[str, dict]:

    if not ARCHIVE_JSON.exists():

        print(
            "[INFO] No existing archive found."
        )

        return {}

    data = json.loads(

        ARCHIVE_JSON.read_text(
            encoding="utf-8"
        )
    )

    if isinstance(data, list):

        rows = data

    elif isinstance(data, dict):

        rows = data.get("articles")

    else:

        raise ValueError(
            "Unrecognized archive structure"
        )

    if not isinstance(rows, list):

        raise ValueError(
            "Archive articles must be a list"
        )

    archive = {}

    for row in rows:

        key = article_fingerprint(row)

        # Compatibility with older archive formats.

        if "source_language" not in row:

            row["source_language"] = SOURCES.get(

                row.get("source", ""),

                {}

            ).get(

                "language",

                "cs"
            )

        if "translation_status" not in row:

            if row["source_language"] == "en":

                row["translation_status"] = (
                    "not_required"
                )

                row["title_en"] = row.get(
                    "title_raw", ""
                )

                row["lead_en"] = row.get(
                    "lead_raw", ""
                )

            elif (
                row.get("title_en")
                and (
                    not row.get("lead_raw")
                    or row.get("lead_en")
                )
            ):

                row["translation_status"] = (
                    "completed"
                )

            else:

                row["translation_status"] = (
                    "pending"
                )

        row.setdefault(
            "translation_attempts",
            0
        )

        row.setdefault(
            "translation_provider",
            None
        )

        row.setdefault(
            "translation_error",
            None
        )

        row.setdefault(
            "translated_at_utc",
            None
        )

        row["fingerprint"] = key

        archive[key] = row

    return archive


def sorted_articles(
    archive: Dict[str, dict]
) -> List[dict]:

    return sorted(

        archive.values(),

        key=lambda article: article.get(
            "published_utc", ""
        ),

        reverse=True
    )


# ============================================================
# JSON OUTPUT
# ============================================================

def atomic_write_json(
    path: Path,
    payload: dict
) -> None:

    path.parent.mkdir(

        parents=True,

        exist_ok=True
    )

    temp_path = path.with_suffix(
        ".tmp"
    )

    temp_path.write_text(

        json.dumps(

            payload,

            ensure_ascii=False,

            indent=2,
        ),

        encoding="utf-8"
    )

    temp_path.replace(path)


def build_payload(
    articles: List[dict],
    now: datetime,
    **extra
) -> dict:

    return {

        "country": "Czech Republic",

        "country_code": "CZ",

        "updated_at_utc": utc_string(now),

        **extra,

        "count": len(articles),

        "articles": articles,
    }


def save_archive(
    archive: Dict[str, dict],
    now: datetime
) -> None:

    atomic_write_json(

        ARCHIVE_JSON,

        build_payload(

            sorted_articles(archive),

            now
        )
    )


def is_publishable(
    article: dict
) -> bool:

    return article.get(

        "translation_status"

    ) in (

        "completed",

        "not_required"
    )


def write_output_files(
    archive: Dict[str, dict],
    now: datetime
) -> None:

    all_articles = sorted_articles(
        archive
    )

    save_archive(
        archive,
        now
    )

    # Latest and topic feeds only contain articles
    # that are ready to display in English.

    latest = [

        article

        for article in all_articles

        if within_window(article, now)

        and is_publishable(article)
    ]

    atomic_write_json(

        OUT_JSON,

        build_payload(

            latest,

            now,

            keep_hours=KEEP_HOURS
        )
    )

    for topic in TOPIC_KEYWORDS:

        subset = [

            article

            for article in latest

            if topic in article.get(
                "topics", []
            )
        ]

        atomic_write_json(

            Path(
                f"data/articles_{topic}.json"
            ),

            build_payload(

                subset,

                now,

                topic=topic
            )
        )

        print(
            f"[OK] {topic}: "
            f"{len(subset)} articles"
        )

    print(
        f"[OK] Latest articles: {len(latest)}"
    )

    print(
        f"[OK] Archive articles: "
        f"{len(all_articles)}"
    )


# ============================================================
# TRANSLATION QUEUE
# ============================================================

def translate_pending_articles(

    archive: Dict[str, dict],

    now: datetime

) -> int:

    pending = [

        article

        for article in archive.values()

        if needs_translation(article)
    ]

    # Prioritize recent news, followed by the older backlog.

    pending.sort(

        key=lambda article: (

            within_window(article, now),

            article.get("published_utc", "")
        ),

        reverse=True
    )

    print(
        f"[INFO] Total pending translations: "
        f"{len(pending)}"
    )

    selected = pending[:TRANSLATE_LIMIT]

    print(
        f"[INFO] Selected for this run: "
        f"{len(selected)}"
    )

    if not selected:

        print(
            "[INFO] No translations required."
        )

        return 0

    load_translator()

    completed = 0

    for start in range(

        0,

        len(selected),

        BATCH_SIZE
    ):

        batch = selected[

            start:start + BATCH_SIZE
        ]

        batch_number = (

            start // BATCH_SIZE + 1
        )

        batch_start = time.monotonic()

        print(
            f"[BATCH] {batch_number}: "
            f"{len(batch)} articles"
        )

        for article in batch:

            article["translation_attempts"] += 1

        try:

            titles = [

                article.get("title_raw", "")

                for article in batch
            ]

            leads = [

                article.get("lead_raw", "")

                for article in batch
            ]

            translated_titles = translate_batch(
                titles
            )

            translated_leads = translate_batch(
                leads
            )

            for article, title_en, lead_en in zip(

                batch,

                translated_titles,

                translated_leads
            ):

                article["title_en"] = title_en

                article["lead_en"] = lead_en

                article["translation_status"] = (
                    "completed"
                )

                article["translation_provider"] = (
                    MODEL_NAME
                )

                article["translated_at_utc"] = (
                    utc_string(utcnow())
                )

                article["translation_error"] = None

                # Reclassify using both Czech and English.

                classify_topics(

                    article,

                    include_translation=True
                )

                completed += 1

                print(
                    f"[TRANSLATED] "
                    f"{article['title_raw']}"
                )

                print(
                    f"          -> {title_en}"
                )

                print(
                    f"[TOPICS] "
                    f"{article['topics']}"
                )

        except Exception as exc:

            print(
                f"[ERROR] Batch translation failed: "
                f"{type(exc).__name__}: {exc}"
            )

            for article in batch:

                article["translation_status"] = (
                    "failed"
                )

                article["translation_error"] = str(
                    exc
                )

            save_archive(
                archive,
                now
            )

            # Stop to avoid repeatedly failing
            # with an unavailable or broken model.

            break

        # Save completed translations immediately.

        save_archive(

            archive,

            now
        )

        elapsed = (

            time.monotonic() - batch_start
        )

        print(
            f"[BATCH] Completed in "
            f"{elapsed:.1f}s"
        )

        print(
            f"[PROGRESS] "
            f"{completed}/{len(selected)} "
            f"articles translated"
        )

    return completed


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    now = utcnow()

    start_time = time.monotonic()

    print("=" * 65)

    print(
        "[INFO] Czech Media Tracker"
    )

    print(
        f"[INFO] Run time: {utc_string(now)}"
    )

    print(
        f"[INFO] Model: {MODEL_NAME}"
    )

    print(
        f"[INFO] Rolling window: "
        f"{KEEP_HOURS} hours"
    )

    print(
        f"[INFO] Translation limit: "
        f"{TRANSLATE_LIMIT} articles"
    )

    print(
        f"[INFO] Batch size: {BATCH_SIZE}"
    )

    print(
        f"[INFO] Translate all: {TRANSLATE_ALL}"
    )

    print("=" * 65)

    # --------------------------------------------------------
    # 1. Load persistent archive.
    # --------------------------------------------------------

    archive = load_archive()

    existing_count = len(archive)

    completed_before = sum(

        1

        for article in archive.values()

        if article.get("translation_status")
        == "completed"
    )

    print(
        f"[INFO] Existing archive: "
        f"{existing_count} articles"
    )

    print(
        f"[INFO] Previously translated: "
        f"{completed_before} articles"
    )

    # --------------------------------------------------------
    # 2. Collect new RSS articles.
    # --------------------------------------------------------

    new_count = 0
    duplicate_count = 0
    new_english_count = 0
    new_pending_count = 0
    new_irrelevant_count = 0

    successful_sources = 0

    for source_name, config in SOURCES.items():

        print(
            f"[RSS] Fetching {source_name}"
        )

        try:

            rows = collect_from_rss(

                source_name,

                config,

                now
            )

        except Exception as exc:

            print(
                f"[ERROR] RSS source failed: "
                f"{source_name}: {exc}"
            )

            continue

        successful_sources += 1

        print(
            f"[RSS] {source_name}: "
            f"{len(rows)} recent articles"
        )

        for row in rows:

            key = article_fingerprint(
                row
            )

            # Never reprocess an article already stored.

            if key in archive:

                duplicate_count += 1

                continue

            article = prepare_article(
                row
            )

            archive[key] = article

            new_count += 1

            status = article[
                "translation_status"
            ]

            if status == "not_required":

                new_english_count += 1

            elif status == "pending":

                new_pending_count += 1

            elif status == "not_relevant":

                new_irrelevant_count += 1

    print("=" * 65)

    print(
        f"[INFO] New articles: {new_count}"
    )

    print(
        f"[INFO] Duplicates skipped: "
        f"{duplicate_count}"
    )

    print(
        f"[INFO] New English articles: "
        f"{new_english_count}"
    )

    print(
        f"[INFO] New Czech translation candidates: "
        f"{new_pending_count}"
    )

    print(
        f"[INFO] New irrelevant Czech articles: "
        f"{new_irrelevant_count}"
    )

    # --------------------------------------------------------
    # 3. Save collection before loading the model.
    # --------------------------------------------------------

    save_archive(
        archive,
        now
    )

    write_output_files(
        archive,
        now
    )

    print(
        "[OK] Original articles saved."
    )

    # --------------------------------------------------------
    # 4. Reconsider untranslated articles if settings
    #    or keywords have changed.
    # --------------------------------------------------------

    for article in archive.values():

        if article.get("translation_status") == (
            "not_relevant"
        ):

            classify_topics(

                article,

                include_translation=False
            )

            if (
                TRANSLATE_ALL
                or article["relevant"]
            ):

                article["translation_status"] = (
                    "pending"
                )

    save_archive(
        archive,
        now
    )

    # --------------------------------------------------------
    # 5. Translate pending articles.
    # --------------------------------------------------------

    translated_count = 0

    try:

        translated_count = translate_pending_articles(

            archive,

            now
        )

    except Exception as exc:

        print(
            f"[ERROR] Translation stage failed: "
            f"{type(exc).__name__}: {exc}"
        )

        print(
            "[WARN] Articles remain saved "
            "for future translation."
        )

    # --------------------------------------------------------
    # 6. Generate all final output files.
    # --------------------------------------------------------

    write_output_files(

        archive,

        utcnow()
    )

    # --------------------------------------------------------
    # 7. Final statistics.
    # --------------------------------------------------------

    statuses = {

        "completed": 0,

        "pending": 0,

        "failed": 0,

        "not_required": 0,

        "not_relevant": 0,
    }

    for article in archive.values():

        status = article.get(
            "translation_status",
            "pending"
        )

        statuses[status] = (

            statuses.get(status, 0) + 1
        )

    elapsed = (

        time.monotonic() - start_time
    )

    print("=" * 65)

    print(
        "[OK] Czech Media Tracker completed"
    )

    print(
        f"[OK] New articles collected: "
        f"{new_count}"
    )

    print(
        f"[OK] Newly translated articles: "
        f"{translated_count}"
    )

    print(
        f"[OK] Total translated articles: "
        f"{statuses['completed']}"
    )

    print(
        f"[OK] Original English articles: "
        f"{statuses['not_required']}"
    )

    print(
        f"[INFO] Pending translations: "
        f"{statuses['pending']}"
    )

    print(
        f"[INFO] Failed translations: "
        f"{statuses['failed']}"
    )

    print(
        f"[INFO] Irrelevant articles archived: "
        f"{statuses['not_relevant']}"
    )

    print(
        f"[OK] Total archived articles: "
        f"{len(archive)}"
    )

    print(
        f"[INFO] Runtime: "
        f"{elapsed:.1f} seconds"
    )

    print("=" * 65)

    return 0


if __name__ == "__main__":

    raise SystemExit(main())
