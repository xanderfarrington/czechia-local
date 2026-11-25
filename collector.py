# -*- coding: utf-8 -*-
"""
Czech Media Tracker — Version 2

Features:
- RSS collection and persistent article archive
- NLLB-200 distilled 1.3B Czech-to-English translation
- Sentence-by-sentence translation
- Length-aware sentence batching
- Persistent sentence translation cache
- Czech proper noun and institutional glossary
- Translation validation and retry
- Improved keyword matching
- Auditable topic classification
- Selective repair of older translations
- Independent processing of English-language articles
"""

from __future__ import annotations

import os
import re
import json
import time
import html
import hashlib
import unicodedata

from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import Counter
from decimal import Decimal, InvalidOperation

import requests
import feedparser
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

KEEP_HOURS = int(os.getenv("KEEP_HOURS", "48"))

TRANSLATE_LIMIT = int(
    os.getenv("TRANSLATE_LIMIT", "60")
)

REPAIR_LIMIT = int(
    os.getenv("REPAIR_LIMIT", "20")
)

BATCH_SIZE = max(
    1, int(os.getenv("BATCH_SIZE", "8"))
)

ARTICLE_GROUP_SIZE = max(
    1, int(os.getenv("ARTICLE_GROUP_SIZE", "8"))
)

NUM_BEAMS = max(
    1, int(os.getenv("NUM_BEAMS", "4"))
)

CPU_THREADS = max(
    1, int(os.getenv("CPU_THREADS", "4"))
)

MAX_RUNTIME_MINUTES = int(
    os.getenv("MAX_RUNTIME_MINUTES", "40")
)

TRANSLATION_VERSION = 2
CLASSIFICATION_VERSION = 2

MODEL_NAME = os.getenv(
    "MODEL_NAME",
    "facebook/nllb-200-distilled-1.3B"
)

TRANSLATE_ALL = (
    os.getenv("TRANSLATE_ALL", "0") == "1"
)

RETRANSLATE_INCOMPLETE = (
    os.getenv("RETRANSLATE_INCOMPLETE", "1") == "1"
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

SENTENCE_CACHE_JSON = Path(
    os.getenv(
        "SENTENCE_CACHE_JSON",
        "data/sentence_translation_cache.json"
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
}

TOKENIZER = None
MODEL = None
TORCH = None

RUN_START = time.monotonic()

CACHE_HITS = 0
CACHE_MISSES = 0
MODEL_SENTENCES = 0


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
#
# exact:
#   Whole words and complete phrases.
#
# stems:
#   Match the beginning of Czech words, allowing
#   grammatical endings without arbitrary substring matches.
# ============================================================

TOPIC_KEYWORDS = {

    "diplomacy": {

        "exact": [
            "foreign ministry",
            "ministry of foreign affairs",
            "foreign policy",
            "diplomacy",
            "diplomatic",
            "diplomatic relations",
            "international relations",
            "peace talks",
            "peace negotiations",
            "bilateral talks",
            "bilateral meeting",
            "summit",
            "ambassador",
            "ambassadors",
            "embassy",
            "embassies",
            "envoy",
            "bilateral",
            "multilateral",
            "strategic partnership",
            "joint statement",
            "state visit",
            "diplomatic mission",
            "foreign ministers",
            "foreign minister",
            "United Nations",
            "European Council",
            "ministerstvo zahraničních věcí",
            "zahraniční politika",
            "mírová jednání",
            "mezinárodní vztahy",
            "státní návštěva",
            "OSN",
            "MZV",
        ],

        "stems": [
            "diplomac",
            "diplomat",
            "velvyslan",
            "ambasád",
            "bilaterál",
            "multilaterál",
            "vyslanec",
            "vyslanc",
            "summit",
        ],
    },

    "military": {

        "exact": [
            "military",
            "armed forces",
            "defense ministry",
            "defence ministry",
            "ministry of defense",
            "ministry of defence",
            "defense spending",
            "defence spending",
            "defense industry",
            "defence industry",
            "military aid",
            "military exercise",
            "military exercises",
            "military equipment",
            "military intelligence",
            "military operations",
            "troops",
            "soldier",
            "soldiers",
            "missile",
            "missiles",
            "air defense",
            "air defence",
            "navy",
            "submarine",
            "submarines",
            "weapons",
            "ammunition",
            "artillery",
            "battlefield",
            "combat",
            "war",
            "warfare",
            "NATO",
            "BIS",
            "cyber attack",
            "cyber attacks",
            "cybersecurity",
            "hybrid warfare",
            "hybrid threats",
            "armed conflict",
            "ministerstvo obrany",
            "generální štáb",
            "vojenské zpravodajství",
            "ozbrojené síly",
            "hybridní válka",
            "hybridní hrozby",
            "kybernetický útok",
        ],

        "stems": [
            "armád",
            "voják",
            "vojác",
            "vojensk",
            "munic",
            "dělostřel",
            "protivzduš",
            "letectv",
            "zbran",
            "zbraň",
            "výzbroj",
            "zbroj",
            "obrann",
            "bojišt",
            "bojov",
            "váleč",
            "válk",
            "raket",
            "kybernetick",
            "bezpilot",
            "pěchot",
        ],
    },

    "energy": {

        "exact": [
            "energy",
            "electricity",
            "oil",
            "gas",
            "natural gas",
            "LNG",
            "pipeline",
            "pipelines",
            "refinery",
            "refineries",
            "power grid",
            "power plant",
            "power plants",
            "nuclear power",
            "nuclear energy",
            "nuclear reactor",
            "nuclear reactors",
            "coal",
            "fuel",
            "petroleum",
            "energy security",
            "district heating",
            "renewable energy",
            "solar power",
            "wind power",
            "energy crisis",
            "ČEZ",
            "CEZ",
            "Dukovany",
            "Temelín",
            "Temelin",
            "ropa",
            "plyn",
            "uhlí",
            "elektřina",
            "energetická bezpečnost",
            "přenosová soustava",
        ],

        "stems": [
            "energet",
            "energi",
            "ropovod",
            "ropn",
            "plynovod",
            "plynov",
            "rafineri",
            "elektřin",
            "elektrárn",
            "jadern",
            "uhl",
            "paliv",
            "dukovan",
            "temelín",
            "teplár",
            "emisní",
            "fotovolta",
        ],
    },

    "economy": {

        "exact": [
            "economy",
            "economic",
            "GDP",
            "inflation",
            "interest rate",
            "interest rates",
            "central bank",
            "Czech National Bank",
            "trade",
            "trade deal",
            "trade deals",
            "exports",
            "imports",
            "industry",
            "manufacturing",
            "investment",
            "investments",
            "budget",
            "deficit",
            "banking",
            "bank",
            "banks",
            "sanctions",
            "stock market",
            "stock exchange",
            "employment",
            "unemployment",
            "unemployment benefits",
            "wages",
            "salary",
            "salaries",
            "tax",
            "taxes",
            "tariff",
            "tariffs",
            "mortgage",
            "mortgages",
            "public finances",
            "fiscal policy",
            "monetary policy",
            "government debt",
            "national debt",
            "ČNB",
            "CNB",
            "HDP",
            "clo",
            "cla",
            "cel",
            "česká národní banka",
            "státní rozpočet",
            "fiskální politika",
            "měnová politika",
        ],

        "stems": [
            "ekonom",
            "inflac",
            "úrok",
            "korun",
            "obchod",
            "export",
            "vývoz",
            "import",
            "dovoz",
            "průmysl",
            "investic",
            "rozpočt",
            "schodk",
            "deficit",
            "bankov",
            "bank",
            "sankc",
            "zaměstnan",
            "nezaměstnan",
            "mzd",
            "platov",
            "daň",
            "daní",
            "daňov",
            "hypoték",
            "burz",
            "odbor",
            "fiskáln",
            "důchod",
            "cenov",
        ],
    },

    "local_events": {

        "exact": [
            "fire",
            "fires",
            "flood",
            "floods",
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
            "traffic accident",
            "natural disaster",
            "injured",
            "firefighters",
            "emergency services",
            "mimořádná událost",
        ],

        "stems": [
            "požár",
            "povod",
            "záplav",
            "zemětřesen",
            "bouř",
            "vichřic",
            "výbuch",
            "nehod",
            "havári",
            "evakuac",
            "katastrof",
            "sesuv",
            "výpadek",
            "zřícen",
            "záchran",
            "zraněn",
            "hasič",
        ],
    },

    "domestic_politics": {

        "exact": [
            "Czech government",
            "Czech parliament",
            "Czech president",
            "Czech prime minister",
            "Chamber of Deputies",
            "parliamentary election",
            "parliamentary elections",
            "presidential election",
            "presidential elections",
            "governing coalition",
            "coalition government",
            "opposition party",
            "opposition parties",
            "cabinet",
            "legislation",
            "confidence vote",
            "no-confidence vote",
            "government formation",
            "political party",
            "political parties",
            "constitutional court",
            "ANO",
            "ODS",
            "SPD",
            "STAN",
            "TOP 09",
            "KDU-ČSL",
            "Poslanecká sněmovna",
            "Ústavní soud",
            "vládní koalice",
            "politická strana",
            "hlasování o důvěře",
            "hlasování o nedůvěře",
        ],

        "stems": [
            "sněmovn",
            "poslan",
            "senátor",
            "parlament",
            "koalic",
            "opozic",
            "premiér",
            "ministr",
            "prezident",
            "voleb",
            "volič",
            "legislativ",
            "ústavn",
            "referend",
            "hejtm",
            "zastupitel",
            "vlád",
            "demis",
            "politick",
        ],
    },
}


TOPIC_THRESHOLDS = {
    "diplomacy": 1,
    "military": 1,
    "energy": 1,
    "economy": 1,
    "local_events": 1,
    "domestic_politics": 1,
}

# Lower-confidence keywords cannot establish a topic alone.

WEAK_KEYWORDS = {

    "economy": {
        "bank",
        "korun",
        "obchod",
        "budget",
        "rozpočt",
        "důchod",
        "industry",
    },

    "domestic_politics": {
        "vlád",
        "ministr",
        "prezident",
        "cabinet",
        "politick",
        "parlament",
    },

    "local_events": {
        "injured",
        "emergency",
        "zraněn",
        "záchran",
    },
}


# ============================================================
# PROPER NOUN AND TERMINOLOGY GLOSSARY
#
# Each entry:
#   Czech regex pattern
#   English replacement
#
# Place names and surnames are deliberately preserved.
# Context-dependent words are not blindly replaced.
# ============================================================

GLOSSARY = [

    # Political figures

    (r"\bBabiš(?:e|ovi|em|ův|ova|ovo|ovy|ových)?\b",
     "Babiš"),

    (r"\bSchillerov(?:á|é|ou|ě|y)\b",
     "Schillerová"),

    (r"\bOkamur(?:a|y|ovi|ou|ův|ova|ovo|ovy)\b",
     "Okamura"),

    (r"\bFial(?:a|y|ovi|ou|ův|ova|ovo)\b",
     "Fiala"),

    (r"\bFic(?:o|a|ovi|em|ův|ova|ovo)\b",
     "Fico"),

    (r"\bHavlíčk(?:a|ovi|em|ův|ova|ovo)\b",
     "Havlíček"),

    (r"\bHavlíček\b",
     "Havlíček"),

    (r"\bJuchelk(?:a|y|ovi|ou)\b",
     "Juchelka"),

    (r"\bStředul(?:a|y|ovi|ou)\b",
     "Středula"),

    (r"\bMacink(?:a|y|ovi|ou)\b",
     "Macinka"),

    (r"\bLipavsk(?:ý|ého|ému|ým)\b",
     "Lipavský"),

    (r"\bPetr(?:a|ovi|em)? Pav(?:el|la|lovi|lem)\b",
     "Petr Pavel"),

    # Czech places

    (r"\bVelk(?:é|ého|ém|ým) Meziříčí\b",
     "Velké Meziříčí"),

    (r"\b(?:Dvůr|Dvora|Dvoře|Dvorem) "
     r"Králové nad Labem\b",
     "Dvůr Králové nad Labem"),

    (r"\bNov(?:ý|ého|ém|ým) Jičín(?:a|ě|em)?\b",
     "Nový Jičín"),

    (r"\bDukovan(?:y|ech|ům|ami)?\b",
     "Dukovany"),

    (r"\bTemelín(?:a|ě|em|u)?\b",
     "Temelín"),

    (r"\bPřerovsk(?:o|a|u|em)\b",
     "Přerov region"),

    (r"\bAlžbětin(?:y|ých|ým) lázn(?:ě|í|ích)\b",
     "Alžbětiny Lázně"),

    # Institutions

    (r"\bČNB\b",
     "Czech National Bank"),

    (r"\bMPSV\b",
     "Ministry of Labour and Social Affairs"),

    (r"\bMPO\b",
     "Ministry of Industry and Trade"),

    (r"\bMZV\b",
     "Ministry of Foreign Affairs"),

    (r"\bČRo\b",
     "Czech Radio"),

    (r"\bČT\b",
     "Czech Television"),

    (r"\bČTK\b",
     "Czech News Agency"),

    (r"\bČEZ\b",
     "ČEZ"),

    (r"\bBIS\b",
     "Security Information Service"),

    (r"\bPoslaneck(?:á|é|ou|é) sněmovn"
     r"(?:a|y|ě|u|ou)\b",
     "Chamber of Deputies"),

    (r"\bSněmovn(?:a|y|ě|u|ou)\b",
     "Chamber of Deputies"),

    # Press conference shorthand.
    # Restrict TK expansion to headline-like contexts.

    (r"(?<=ŽIVĚ: )TK\b",
     "press conference"),

    (r"^TK(?=\s)",
     "Press conference"),
]


# Known incorrect outputs observed in Version 1.

KNOWN_BAD_TRANSLATIONS = [

    (
        r"\bBabiš",
        r"\bgrandma\b|\bgrandmother\b"
    ),

    (
        r"\bPavel\b",
        r"\bPaul\b"
    ),

    (
        r"Meziříčí",
        r"\bGrand Canyon\b"
    ),

    (
        r"Králové nad Labem",
        r"\bPalace of the Kings\b"
    ),

    (
        r"hejtman",
        r"\bchieftains?\b"
    ),

    (
        r"odvolání",
        r"\bresignation\b"
    ),

    (
        r"teplár",
        r"\bboilers\b"
    ),
]


# ============================================================
# GENERAL HELPERS
# ============================================================

def utcnow():

    return datetime.now(timezone.utc)


def utc_string(dt):

    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def normalize_space(text):

    return " ".join(
        (text or "").split()
    ).strip()


def clean_text(text):

    text = html.unescape(text or "")

    text = re.sub(
        r"<[^>]+>",
        " ",
        text
    )

    return normalize_space(text)


def normalize_url(url):

    url = normalize_space(url)

    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)

    return url.rstrip("/")


def normalize_title(text):

    text = normalize_space(
        (text or "").casefold()
    )

    return re.sub(
        r"[^\w\s]",
        "",
        text
    )


def fold_accents(text):

    return "".join(

        char

        for char in unicodedata.normalize(
            "NFKD", text or ""
        )

        if not unicodedata.combining(char)

    ).casefold()


def hash_key(*parts):

    h = hashlib.sha1()

    for part in parts:

        h.update(
            (part or "").encode("utf-8")
        )

        h.update(b"|")

    return h.hexdigest()


def article_fingerprint(article):

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


def within_window(article, now):

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


def runtime_exceeded():

    elapsed = (
        time.monotonic() - RUN_START
    )

    return elapsed >= (
        MAX_RUNTIME_MINUTES * 60
    )


# ============================================================
# SENTENCE SPLITTING
#
# Protect common Czech abbreviations, decimal numbers,
# initials and ordinal numbers before splitting.
# ============================================================

ABBREVIATIONS = [

    "např.",
    "tj.",
    "tzn.",
    "tzv.",
    "apod.",
    "atd.",
    "resp.",
    "pozn.",
    "č.",
    "odst.",
    "str.",
    "sb.",
    "mil.",
    "mld.",
    "tis.",
    "ing.",
    "mgr.",
    "mudr.",
    "judr.",
    "phdr.",
    "rndr.",
    "prof.",
    "doc.",
    "st.",
    "sv.",
    "a.s.",
    "s.r.o.",
    "v.s.",
    "U.S.",
    "U.K.",
]


def split_sentences(text):

    text = normalize_space(text)

    if not text:
        return []

    marker = "\uE000"

    protected = text

    # Protect decimal points between numbers.

    protected = re.sub(

        r"(?<=\d)\.(?=\d)",

        marker,

        protected
    )

    # Protect ordinal numbers such as "25. listopadu".

    protected = re.sub(

        r"(?<=\d)\.(?=\s+[a-záčďéěíňóřšťúůýž])",

        marker,

        protected
    )

    # Protect known abbreviations.

    for abbreviation in sorted(
        ABBREVIATIONS,
        key=len,
        reverse=True
    ):

        pattern = (
            r"(?<!\w)"
            + re.escape(abbreviation)
        )

        protected = re.sub(

            pattern,

            lambda match: match.group(0).replace(
                ".", marker
            ),

            protected,

            flags=re.IGNORECASE
        )

    # Protect single-letter initials before names.

    protected = re.sub(

        r"\b([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ])\."
        r"(?=\s+[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ])",

        lambda match: match.group(1) + marker,

        protected
    )

    # Split after sentence-ending punctuation, including
    # punctuation followed by closing quotation marks.

    parts = re.split(

        r'(?<=[.!?])\s+|'
        r'(?<=[.!?]["”’»])\s+',

        protected
    )

    sentences = [

        normalize_space(
            part.replace(marker, ".")
        )

        for part in parts

        if part.strip()
    ]

    return sentences


def two_sentence_lead(text):

    sentences = split_sentences(
        clean_text(text)
    )

    if not sentences:
        return ""

    # Preserve complete sentences rather than cutting
    # the second sentence at an arbitrary character count.

    return " ".join(
        sentences[:2]
    )


def join_sentences(original_sentences, translations):

    output = []

    for index, translated in enumerate(translations):

        translated = normalize_space(translated)

        if not translated:
            continue

        # Ensure separately translated sentences do not
        # become accidentally joined without punctuation.

        if index < len(translations) - 1:

            if not re.search(
                r'[.!?]["”’»]?$',
                translated
            ):

                translated += "."

        output.append(translated)

    return " ".join(output)


# ============================================================
# RSS COLLECTION
# ============================================================

def fetch_text(url):

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


def parse_entry_datetime(entry):

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


def collect_from_rss(source_name, config, now):

    rows = []

    for feed_url in config["feeds"]:

        raw_feed = fetch_text(feed_url)

        if not raw_feed:
            continue

        feed = feedparser.parse(raw_feed)

        print(
            f"[RSS] {source_name}: "
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

            row = {

                "source": source_name,

                "source_language": config["language"],

                "url": normalize_url(
                    entry.get("link", "")
                ),

                "published_utc": utc_string(
                    published
                ),

                "title_raw": title,

                "lead_raw": two_sentence_lead(

                    entry.get("summary", "")

                    or entry.get("description", "")

                    or ""
                ),
            }

            if within_window(row, now):

                rows.append(row)

        if rows:
            break

    return rows


# ============================================================
# KEYWORD CLASSIFICATION
# ============================================================

def exact_match(keyword, text):

    # Acronyms are matched case-sensitively so, for
    # example, political party ANO does not match "ano".

    is_acronym = (

        len(keyword) <= 12

        and keyword.isupper()
    )

    pattern = (

        r"(?<!\w)"

        + re.escape(keyword)

        + r"(?!\w)"
    )

    flags = (

        0

        if is_acronym

        else re.IGNORECASE
    )

    return bool(
        re.search(
            pattern,
            text,
            flags
        )
    )


def stem_match(stem, text):

    pattern = (

        r"(?<!\w)"

        + re.escape(stem)

        + r"\w*"
    )

    return bool(

        re.search(

            pattern,

            text,

            flags=re.IGNORECASE
        )
    )


def classify_topics(article, include_translation=True):

    raw_text = (

        f"{article.get('title_raw') or ''} "

        f"{article.get('lead_raw') or ''}"
    )

    english_text = ""

    if include_translation:

        english_text = (

            f"{article.get('title_en') or ''} "

            f"{article.get('lead_en') or ''}"
        )

    combined_text = (

        raw_text + " " + english_text
    )

    scores = {}
    matched_keywords = {}
    labels = []

    for topic, config in TOPIC_KEYWORDS.items():

        matches = set()

        for keyword in config["exact"]:

            if exact_match(
                keyword,
                combined_text
            ):

                matches.add(keyword)

        # Czech stems are matched only against
        # original Czech text.

        if article.get("source_language") != "en":

            for stem in config["stems"]:

                if stem_match(
                    stem,
                    raw_text
                ):

                    matches.add(stem + "*")

        weak = WEAK_KEYWORDS.get(
            topic,
            set()
        )

        strong_matches = [

            match

            for match in matches

            if match.rstrip("*") not in weak
        ]

        score = len(matches)

        scores[topic] = score

        matched_keywords[topic] = sorted(
            matches
        )

        qualifies = (

            score >= TOPIC_THRESHOLDS[topic]

            and (

                bool(strong_matches)

                or score >= 2
            )
        )

        if qualifies:

            labels.append(topic)

    article["topic_scores"] = scores
    article["matched_keywords"] = matched_keywords
    article["topics"] = labels
    article["relevant"] = bool(labels)

    article["primary_topic"] = (

        max(
            labels,
            key=lambda topic: scores[topic]
        )

        if labels

        else None
    )

    article["classification_version"] = (
        CLASSIFICATION_VERSION
    )

    return article


# ============================================================
# GLOSSARY HELPERS
# ============================================================

def expected_glossary_terms(text):

    expected = []

    for pattern, replacement in GLOSSARY:

        if re.search(
            pattern,
            text or ""
        ):

            expected.append(replacement)

    return sorted(set(expected))


def protect_terms(text):

    """
    Replace known names and institutional terms with
    numbered placeholders.

    Placeholders are validated after inference.
    If the model drops them, the translation is retried
    using a different preprocessing strategy.
    """

    replacements = {}

    protected = text

    counter = 0

    for pattern, replacement in GLOSSARY:

        def replace_match(match):

            nonlocal counter

            token = f"ZXQ{counter}QXZ"

            counter += 1

            replacements[token] = replacement

            return token

        protected = re.sub(

            pattern,

            replace_match,

            protected
        )

    return protected, replacements


def restore_terms(translated, replacements):

    output = translated

    missing = []

    for token, replacement in replacements.items():

        # Allow whitespace between placeholder letters,
        # which some tokenizers may introduce.

        pattern = r"\s*".join(

            re.escape(character)

            for character in token
        )

        output, count = re.subn(

            pattern,

            lambda match: replacement,

            output,

            flags=re.IGNORECASE
        )

        if count != 1:

            missing.append(replacement)

    return normalize_space(output), missing


def glossary_expansion(text):

    """
    Alternative retry strategy: supply recognized English
    terminology directly instead of placeholders.

    This is validated afterward rather than assumed correct.
    """

    output = text

    for pattern, replacement in GLOSSARY:

        output = re.sub(

            pattern,

            lambda match: replacement,

            output
        )

    return output


# ============================================================
# TRANSLATION VALIDATION
# ============================================================

def extract_numbers(text):

    text = (text or "").replace(
        "\u00a0",
        " "
    )

    # Normalize thousands separated by spaces.

    text = re.sub(

        r"(?<=\d) (?=\d{3}(?:\D|$))",

        "",

        text
    )

    matches = re.findall(

        r"(?<!\w)\d+(?:[.,]\d+)*(?!\w)",

        text
    )

    numbers = []

    for value in matches:

        if "," in value and "." in value:

            # Determine which separator is likely decimal.

            if value.rfind(",") > value.rfind("."):

                value = value.replace(".", "")
                value = value.replace(",", ".")

            else:

                value = value.replace(",", "")

        elif "," in value:

            parts = value.split(",")

            if (
                len(parts) > 2
                or len(parts[-1]) == 3
            ):

                value = value.replace(",", "")

            else:

                value = value.replace(",", ".")

        try:

            value = str(
                Decimal(value).normalize()
            )

        except InvalidOperation:

            pass

        numbers.append(value)

    return Counter(numbers)


def validate_translation(original, translated):

    warnings = []

    original = normalize_space(original)
    translated = normalize_space(translated)

    if not original:
        return warnings

    if not translated:

        return ["empty_translation"]

    original_words = len(
        original.split()
    )

    translated_words = len(
        translated.split()
    )

    # Detect suspiciously short output.

    if original_words >= 8:

        if translated_words < original_words * 0.45:

            warnings.append(
                "suspiciously_short"
            )

    # Detect missing numerical values.

    original_numbers = extract_numbers(
        original
    )

    translated_numbers = extract_numbers(
        translated
    )

    missing_numbers = (

        original_numbers - translated_numbers
    )

    if missing_numbers:

        warnings.append(

            "missing_numbers:"

            + ",".join(
                sorted(missing_numbers.keys())
            )
        )

    # Detect missing percentage information.

    if (

        "%" in original

        or re.search(
            r"\bprocent",
            original,
            flags=re.IGNORECASE
        )
    ):

        if not (

            "%" in translated

            or re.search(

                r"\bpercent(?:age)?\b",

                translated,

                flags=re.IGNORECASE
            )
        ):

            warnings.append(
                "missing_percentage"
            )

    # Validate known names and institutions.

    normalized_translation = fold_accents(
        translated
    )

    for expected in expected_glossary_terms(
        original
    ):

        if fold_accents(expected) not in normalized_translation:

            warnings.append(

                "missing_glossary_term:"

                + expected
            )

    # Detect known Version 1 mistranslations.

    for source_pattern, bad_pattern in KNOWN_BAD_TRANSLATIONS:

        if (

            re.search(
                source_pattern,
                original,
                flags=re.IGNORECASE
            )

            and re.search(
                bad_pattern,
                translated,
                flags=re.IGNORECASE
            )
        ):

            warnings.append(
                "known_mistranslation"
            )

    # Detect unremoved protection tokens.

    if re.search(

        r"ZXQ\s*\d+\s*QXZ",

        translated,

        flags=re.IGNORECASE
    ):

        warnings.append(
            "unrestored_placeholder"
        )

    # Detect largely unchanged Czech text.
    # Proper nouns alone should not trigger this check.

    if (

        original_words >= 6

        and original.casefold() == translated.casefold()
    ):

        warnings.append(
            "untranslated_text"
        )

    return sorted(set(warnings))


# ============================================================
# JSON AND CACHE HELPERS
# ============================================================

def atomic_write_json(path, payload):

    path.parent.mkdir(

        parents=True,

        exist_ok=True
    )

    temporary = path.with_suffix(
        ".tmp"
    )

    temporary.write_text(

        json.dumps(

            payload,

            ensure_ascii=False,

            indent=2
        ),

        encoding="utf-8"
    )

    temporary.replace(path)


def load_sentence_cache():

    if not SENTENCE_CACHE_JSON.exists():

        return {}

    try:

        data = json.loads(

            SENTENCE_CACHE_JSON.read_text(
                encoding="utf-8"
            )
        )

        if isinstance(data, dict):

            return data.get(
                "translations",
                {}
            )

    except Exception as exc:

        print(
            f"[WARN] Sentence cache could not be loaded: "
            f"{exc}"
        )

    return {}


def save_sentence_cache(cache):

    atomic_write_json(

        SENTENCE_CACHE_JSON,

        {
            "model": MODEL_NAME,

            "translation_version": TRANSLATION_VERSION,

            "updated_at_utc": utc_string(
                utcnow()
            ),

            "count": len(cache),

            "translations": cache,
        }
    )


def sentence_cache_key(text):

    return hash_key(

        MODEL_NAME,

        str(TRANSLATION_VERSION),

        str(NUM_BEAMS),

        normalize_space(text)
    )


# ============================================================
# NLLB MODEL
# ============================================================

def load_translator():

    global TOKENIZER
    global MODEL
    global TORCH

    if MODEL is not None:

        return

    print(
        f"[MODEL] Loading {MODEL_NAME}"
    )

    started = time.monotonic()

    import torch

    from transformers import (

        AutoTokenizer,

        AutoModelForSeq2SeqLM
    )

    TORCH = torch

    available_threads = (
        os.cpu_count() or CPU_THREADS
    )

    threads = min(

        CPU_THREADS,

        available_threads
    )

    torch.set_num_threads(
        threads
    )

    TOKENIZER = AutoTokenizer.from_pretrained(

        MODEL_NAME,

        src_lang=SOURCE_LANGUAGE,

        tgt_lang=TARGET_LANGUAGE,
    )

    MODEL = AutoModelForSeq2SeqLM.from_pretrained(

        MODEL_NAME,

        dtype=torch.float32,

        low_cpu_mem_usage=True,
    )

    MODEL.to("cpu")
    MODEL.eval()

    print(

        f"[MODEL] Loaded in "

        f"{time.monotonic() - started:.1f}s"
    )

    print(
        f"[MODEL] CPU threads: {threads}"
    )

    print(
        f"[MODEL] Beam search: {NUM_BEAMS}"
    )


def token_length(text):

    return len(

        TOKENIZER.encode(

            text,

            add_special_tokens=True
        )
    )


def model_translate(texts, beams=None):

    global MODEL_SENTENCES

    if not texts:
        return []

    load_translator()

    lengths = [

        token_length(text)

        for text in texts
    ]

    # Never silently truncate input.

    if max(lengths) > MAX_INPUT_TOKENS:

        raise ValueError(
            "Sentence exceeds model input token limit"
        )

    inputs = TOKENIZER(

        texts,

        return_tensors="pt",

        padding=True,

        truncation=False,
    )

    target_token = TOKENIZER.convert_tokens_to_ids(

        TARGET_LANGUAGE
    )

    if (

        target_token is None

        or target_token == TOKENIZER.unk_token_id
    ):

        raise ValueError(
            "Invalid target language token"
        )

    beam_count = beams or NUM_BEAMS

    # Adaptive output limits preserve sufficient room
    # for English translation while allowing detection
    # of unusually long generations.

    output_limit = min(

        MAX_NEW_TOKENS,

        max(

            64,

            int(max(lengths) * 2.5) + 24
        )
    )

    with TORCH.inference_mode():

        outputs = MODEL.generate(

            **inputs,

            forced_bos_token_id=target_token,

            max_new_tokens=output_limit,

            num_beams=beam_count,

            do_sample=False,

            early_stopping=(beam_count > 1),

            use_cache=True,
        )

    translations = TOKENIZER.batch_decode(

        outputs,

        skip_special_tokens=True
    )

    MODEL_SENTENCES += len(texts)

    results = []

    for sequence, translation in zip(

        outputs,

        translations
    ):

        ids = sequence.tolist()

        eos = TOKENIZER.eos_token_id
        pad = TOKENIZER.pad_token_id

        while ids and ids[-1] == pad:

            ids.pop()

        truncated = (

            len(ids) >= output_limit + 1

            or eos not in ids[1:]
        )

        results.append({

            "text": normalize_space(
                translation
            ),

            "truncated": truncated,
        })

    return results


# ============================================================
# SENTENCE TRANSLATION AND RETRY
# ============================================================

def translate_sentence_batch(sentences, cache):

    global CACHE_HITS
    global CACHE_MISSES

    results = {}

    missing = []

    # Deduplicate identical sentences within the group.

    unique_sentences = list(

        dict.fromkeys(

            normalize_space(sentence)

            for sentence in sentences

            if normalize_space(sentence)
        )
    )

    for sentence in unique_sentences:

        key = sentence_cache_key(
            sentence
        )

        cached = cache.get(key)

        if cached and cached.get("translation"):

            CACHE_HITS += 1

            results[sentence] = {

                "translation": cached["translation"],

                "warnings": [],

                "cached": True,
            }

        else:

            CACHE_MISSES += 1

            missing.append(sentence)

    if not missing:

        return results

    load_translator()

    prepared = []

    for sentence in missing:

        protected, replacements = protect_terms(
            sentence
        )

        prepared.append({

            "original": sentence,

            "protected": protected,

            "replacements": replacements,

            "length": token_length(
                protected
            ),
        })

    # Group sentences of similar lengths together.

    prepared.sort(

        key=lambda row: row["length"]
    )

    for start in range(

        0,

        len(prepared),

        BATCH_SIZE
    ):

        batch = prepared[

            start:start + BATCH_SIZE
        ]

        started = time.monotonic()

        outputs = model_translate([

            row["protected"]

            for row in batch
        ])

        for row, output in zip(

            batch,

            outputs
        ):

            original = row["original"]

            translated, missing_terms = restore_terms(

                output["text"],

                row["replacements"]
            )

            warnings = validate_translation(

                original,

                translated
            )

            if missing_terms:

                warnings.append(
                    "missing_protected_terms"
                )

            if output["truncated"]:

                warnings.append(
                    "generation_truncated"
                )

            results[original] = {

                "translation": translated,

                "warnings": sorted(
                    set(warnings)
                ),

                "cached": False,
            }

        print(

            f"[SENTENCE BATCH] "

            f"{len(batch)} sentences | "

            f"{time.monotonic() - started:.1f}s"
        )

    # Retry only suspicious translations.

    retry_sentences = [

        sentence

        for sentence in missing

        if results[sentence]["warnings"]
    ]

    if retry_sentences:

        print(

            f"[VALIDATION] Retrying "

            f"{len(retry_sentences)} suspicious sentences"
        )

    retry_sentences.sort(

        key=lambda text: token_length(
            glossary_expansion(text)
        )
    )

    for start in range(

        0,

        len(retry_sentences),

        BATCH_SIZE
    ):

        retry_batch = retry_sentences[

            start:start + BATCH_SIZE
        ]

        retry_inputs = [

            glossary_expansion(sentence)

            for sentence in retry_batch
        ]

        retry_outputs = model_translate(

            retry_inputs,

            beams=max(NUM_BEAMS, 5)
        )

        for sentence, output in zip(

            retry_batch,

            retry_outputs
        ):

            translated = output["text"]

            warnings = validate_translation(

                sentence,

                translated
            )

            if output["truncated"]:

                warnings.append(
                    "generation_truncated"
                )

            warnings = sorted(
                set(warnings)
            )

            previous = results[sentence]

            # Prefer the candidate with fewer detected
            # problems, not merely the longer translation.

            if len(warnings) < len(

                previous["warnings"]
            ):

                results[sentence] = {

                    "translation": translated,

                    "warnings": warnings,

                    "cached": False,
                }

    # Cache only sentences that pass validation.

    for sentence in missing:

        result = results[sentence]

        if not result["warnings"]:

            key = sentence_cache_key(
                sentence
            )

            cache[key] = {

                "original": sentence,

                "translation": result["translation"],

                "model": MODEL_NAME,

                "version": TRANSLATION_VERSION,
            }

    return results


# ============================================================
# ARTICLE PREPARATION
# ============================================================

def prepare_article(row):

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

        "fingerprint": article_fingerprint(row),

        "title_en": None,

        "lead_en": None,

        "translation_status": "pending",

        "translation_version": 0,

        "translation_attempts": 0,

        "translation_provider": None,

        "translation_method": None,

        "translation_error": None,

        "translation_warnings": [],

        "translated_at_utc": None,

        "collected_at_utc": utc_string(
            utcnow()
        ),
    }

    classify_topics(

        item,

        include_translation=False
    )

    if row["source_language"] == "en":

        item["title_en"] = row["title_raw"]

        item["lead_en"] = row["lead_raw"]

        item["translation_status"] = (
            "not_required"
        )

        item["translation_provider"] = (
            "original_english"
        )

    elif (

        not item["relevant"]

        and not TRANSLATE_ALL
    ):

        item["translation_status"] = (
            "not_relevant"
        )

    return item


# ============================================================
# ARCHIVE MANAGEMENT
# ============================================================

def load_archive():

    if not ARCHIVE_JSON.exists():

        return {}

    data = json.loads(

        ARCHIVE_JSON.read_text(
            encoding="utf-8"
        )
    )

    rows = (

        data

        if isinstance(data, list)

        else data.get("articles")
    )

    if not isinstance(rows, list):

        raise ValueError(
            "Invalid archive structure"
        )

    archive = {}

    for article in rows:

        key = article_fingerprint(article)

        article.setdefault(

            "source_language",

            SOURCES.get(

                article.get("source", ""),

                {}

            ).get(

                "language",

                "cs"
            )
        )

        if "translation_status" not in article:

            if article["source_language"] == "en":

                article["translation_status"] = (
                    "not_required"
                )

            elif article.get("title_en"):

                article["translation_status"] = (
                    "completed"
                )

            else:

                article["translation_status"] = (
                    "pending"
                )

        article.setdefault(

            "translation_version",

            1 if article["translation_status"] == "completed"

            else 0
        )

        article.setdefault(
            "translation_attempts",
            0
        )

        article.setdefault(
            "translation_warnings",
            []
        )

        article.setdefault(
            "translation_error",
            None
        )

        article.setdefault(
            "repair_attempts",
            0
        )

        article["fingerprint"] = key

        archive[key] = article

    return archive


def sorted_articles(archive):

    return sorted(

        archive.values(),

        key=lambda article: article.get(
            "published_utc",
            ""
        ),

        reverse=True
    )


def build_payload(articles, now, **extra):

    return {

        "country": "Czech Republic",

        "country_code": "CZ",

        "updated_at_utc": utc_string(now),

        **extra,

        "count": len(articles),

        "articles": articles,
    }


def save_archive(archive, now):

    atomic_write_json(

        ARCHIVE_JSON,

        build_payload(

            sorted_articles(archive),

            now
        )
    )


# ============================================================
# SELECTIVE REPAIR OF VERSION 1 TRANSLATIONS
# ============================================================

def identify_repair_reasons(article):

    if article.get("source_language") != "cs":

        return []

    if article.get("translation_status") != "completed":

        return []

    if article.get("translation_version", 0) >= TRANSLATION_VERSION:

        return []

    if article.get("repair_attempts", 0) >= 2:

        return []

    reasons = []

    for raw_field, english_field in (

        ("title_raw", "title_en"),

        ("lead_raw", "lead_en")
    ):

        original = article.get(
            raw_field,
            ""
        )

        translated = article.get(
            english_field,
            ""
        )

        original_sentences = split_sentences(
            original
        )

        translated_sentences = split_sentences(
            translated
        )

        if len(original_sentences) > 1:

            reasons.append(

                f"{raw_field}:legacy_multi_sentence"
            )

        if len(translated_sentences) < len(
            original_sentences
        ):

            reasons.append(

                f"{raw_field}:possible_sentence_omission"
            )

        for warning in validate_translation(

            original,

            translated
        ):

            reasons.append(

                f"{raw_field}:{warning}"
            )

    return sorted(set(reasons))


def update_classifications(archive):

    reconsidered = 0

    for article in archive.values():

        status = article.get(
            "translation_status"
        )

        include_translation = status in (

            "completed",

            "not_required",

            "review_required"
        )

        classify_topics(

            article,

            include_translation=include_translation
        )

        if article.get("source_language") != "cs":

            continue

        if (

            status == "not_relevant"

            and (

                article["relevant"]

                or TRANSLATE_ALL
            )
        ):

            article["translation_status"] = (
                "pending"
            )

            reconsidered += 1

        elif (

            status in ("pending", "failed")

            and not article["relevant"]

            and not TRANSLATE_ALL
        ):

            article["translation_status"] = (
                "not_relevant"
            )

    print(

        f"[CLASSIFICATION] "

        f"{reconsidered} previously irrelevant "

        f"articles reconsidered"
    )


# ============================================================
# ARTICLE TRANSLATION
# ============================================================

def translate_article_group(articles, cache, repair=False):

    sentence_map = {}

    all_sentences = []

    for article in articles:

        key = article["fingerprint"]

        title_sentences = split_sentences(

            article.get("title_raw", "")
        )

        lead_sentences = split_sentences(

            article.get("lead_raw", "")
        )

        sentence_map[key] = {

            "title": title_sentences,

            "lead": lead_sentences,
        }

        all_sentences.extend(
            title_sentences
        )

        all_sentences.extend(
            lead_sentences
        )

    translated_sentences = translate_sentence_batch(

        all_sentences,

        cache
    )

    completed = 0
    flagged = 0

    for article in articles:

        key = article["fingerprint"]

        original_title_sentences = (

            sentence_map[key]["title"]
        )

        original_lead_sentences = (

            sentence_map[key]["lead"]
        )

        title_parts = [

            translated_sentences[sentence]["translation"]

            for sentence in original_title_sentences
        ]

        lead_parts = [

            translated_sentences[sentence]["translation"]

            for sentence in original_lead_sentences
        ]

        title_en = join_sentences(

            original_title_sentences,

            title_parts
        )

        lead_en = join_sentences(

            original_lead_sentences,

            lead_parts
        )

        warnings = []

        for field, sentences in (

            ("title", original_title_sentences),

            ("lead", original_lead_sentences)
        ):

            for index, sentence in enumerate(
                sentences
            ):

                for warning in translated_sentences[

                    sentence

                ]["warnings"]:

                    warnings.append(

                        f"{field}_{index + 1}:{warning}"
                    )

        article["translation_attempts"] = (

            article.get("translation_attempts", 0)

            + 1
        )

        if repair:

            article["repair_attempts"] = (

                article.get("repair_attempts", 0)

                + 1
            )

            # Keep the original translation for comparison.

            if "previous_translation" not in article:

                article["previous_translation"] = {

                    "title_en": article.get("title_en"),

                    "lead_en": article.get("lead_en"),

                    "translation_version": article.get(
                        "translation_version",
                        1
                    ),
                }

        article["title_en"] = title_en

        article["lead_en"] = lead_en

        article["translation_version"] = (
            TRANSLATION_VERSION
        )

        article["translation_provider"] = (
            MODEL_NAME
        )

        article["translation_method"] = (
            "sentence_by_sentence"
        )

        article["translated_at_utc"] = utc_string(
            utcnow()
        )

        article["translation_warnings"] = sorted(
            set(warnings)
        )

        article["translation_error"] = None

        article["sentence_counts"] = {

            "title": len(original_title_sentences),

            "lead": len(original_lead_sentences),
        }

        if warnings:

            article["translation_status"] = (
                "review_required"
            )

            flagged += 1

        else:

            article["translation_status"] = (
                "completed"
            )

            completed += 1

        classify_topics(

            article,

            include_translation=True
        )

        print(

            f"[{'REPAIRED' if repair else 'TRANSLATED'}] "

            f"{article['title_raw']}"
        )

        print(
            f"    -> {title_en}"
        )

        print(
            f"[TOPICS] {article['topics']}"
        )

        print(

            "[MATCHES] "

            + json.dumps(

                {

                    topic: keywords

                    for topic, keywords in article[

                        "matched_keywords"

                    ].items()

                    if keywords
                },

                ensure_ascii=False
            )
        )

        if warnings:

            print(
                f"[REVIEW] {warnings}"
            )

    return completed, flagged


# ============================================================
# TRANSLATION QUEUE
# ============================================================

def process_queue(

    queue,

    archive,

    cache,

    repair=False
):

    completed = 0
    flagged = 0
    processed = 0

    for start in range(

        0,

        len(queue),

        ARTICLE_GROUP_SIZE
    ):

        if runtime_exceeded():

            print(

                "[INFO] Runtime budget reached; "

                "remaining articles retained for next run."
            )

            break

        group = queue[

            start:start + ARTICLE_GROUP_SIZE
        ]

        started = time.monotonic()

        print(

            f"[GROUP] "

            f"{len(group)} "

            f"{'repair' if repair else 'new'} articles"
        )

        try:

            good, needs_review = translate_article_group(

                group,

                cache,

                repair=repair
            )

            completed += good
            flagged += needs_review
            processed += len(group)

        except Exception as exc:

            print(

                f"[ERROR] Translation group failed: "

                f"{type(exc).__name__}: {exc}"
            )

            for article in group:

                article["translation_error"] = str(
                    exc
                )

                if not repair:

                    article["translation_status"] = (
                        "failed"
                    )

            save_archive(

                archive,

                utcnow()
            )

            save_sentence_cache(cache)

            break

        save_archive(

            archive,

            utcnow()
        )

        save_sentence_cache(cache)

        print(

            f"[GROUP] Completed in "

            f"{time.monotonic() - started:.1f}s"
        )

        print(

            f"[PROGRESS] "

            f"{processed}/{len(queue)} articles processed"
        )

    return completed, flagged


# ============================================================
# OUTPUT FILES
# ============================================================

def is_publishable(article):

    # Suspicious translations are retained in the archive
    # for inspection but excluded from the English feeds.

    return article.get(

        "translation_status"

    ) in (

        "completed",

        "not_required"
    )


def write_output_files(archive, now):

    all_articles = sorted_articles(
        archive
    )

    save_archive(
        archive,
        now
    )

    latest = [

        article

        for article in all_articles

        if within_window(article, now)

        and is_publishable(article)

        and (

            article.get("relevant")

            or TRANSLATE_ALL

            or article.get("source_language") == "en"
        )
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
                "topics",
                []
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
            f"[OK] {topic}: {len(subset)} articles"
        )

    print(
        f"[OK] Latest articles: {len(latest)}"
    )

    print(
        f"[OK] Archive articles: {len(all_articles)}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    now = utcnow()

    print("=" * 65)

    print("[INFO] Czech Media Tracker — Version 2")

    print(
        f"[INFO] Run time: {utc_string(now)}"
    )

    print(
        f"[INFO] Model: {MODEL_NAME}"
    )

    print(
        f"[INFO] Rolling window: {KEEP_HOURS} hours"
    )

    print(
        f"[INFO] Translation version: {TRANSLATION_VERSION}"
    )

    print(
        f"[INFO] Translation limit: {TRANSLATE_LIMIT}"
    )

    print(
        f"[INFO] Repair limit: {REPAIR_LIMIT}"
    )

    print(
        f"[INFO] Sentence batch size: {BATCH_SIZE}"
    )

    print(
        f"[INFO] Runtime budget: {MAX_RUNTIME_MINUTES} minutes"
    )

    print("=" * 65)

    # --------------------------------------------------------
    # 1. Load archive and persistent sentence cache.
    # --------------------------------------------------------

    archive = load_archive()

    cache = load_sentence_cache()

    print(
        f"[INFO] Existing archive: {len(archive)} articles"
    )

    print(
        f"[INFO] Cached sentences: {len(cache)}"
    )

    # --------------------------------------------------------
    # 2. Collect new articles.
    # --------------------------------------------------------

    new_count = 0
    duplicate_count = 0
    new_english = 0
    new_pending = 0
    new_irrelevant = 0

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

        print(

            f"[RSS] {source_name}: "

            f"{len(rows)} recent articles"
        )

        for row in rows:

            key = article_fingerprint(
                row
            )

            if key in archive:

                duplicate_count += 1

                continue

            article = prepare_article(
                row
            )

            archive[key] = article

            new_count += 1

            status = article["translation_status"]

            if status == "not_required":

                new_english += 1

            elif status == "pending":

                new_pending += 1

            else:

                new_irrelevant += 1

    print(
        f"[INFO] New articles: {new_count}"
    )

    print(
        f"[INFO] Duplicates skipped: {duplicate_count}"
    )

    print(
        f"[INFO] New English articles: {new_english}"
    )

    print(
        f"[INFO] New translation candidates: {new_pending}"
    )

    print(
        f"[INFO] New irrelevant articles: {new_irrelevant}"
    )

    # --------------------------------------------------------
    # 3. Reclassify the existing archive using Version 2.
    # --------------------------------------------------------

    update_classifications(
        archive
    )

    save_archive(
        archive,
        now
    )

    write_output_files(
        archive,
        now
    )

    print(
        "[OK] Original articles and classifications saved."
    )

    # --------------------------------------------------------
    # 4. Identify new translation candidates.
    # --------------------------------------------------------

    pending = [

        article

        for article in archive.values()

        if article.get("source_language") == "cs"

        and article.get("translation_status") in (

            "pending",

            "failed"
        )
    ]

    pending.sort(

        key=lambda article: (

            within_window(article, now),

            article.get("published_utc", "")
        ),

        reverse=True
    )

    pending_queue = pending[:TRANSLATE_LIMIT]

    print(
        f"[INFO] Pending translations: {len(pending)}"
    )

    print(
        f"[INFO] Selected new translations: {len(pending_queue)}"
    )

    # --------------------------------------------------------
    # 5. Identify older translations requiring repair.
    # --------------------------------------------------------

    repair_candidates = []

    if RETRANSLATE_INCOMPLETE:

        for article in archive.values():

            if not (

                article.get("relevant")

                or TRANSLATE_ALL
            ):

                continue

            reasons = identify_repair_reasons(
                article
            )

            if reasons:

                article["repair_reasons"] = reasons

                repair_candidates.append(
                    article
                )

        repair_candidates.sort(

            key=lambda article: (

                within_window(article, now),

                len(
                    article.get("repair_reasons", [])
                ),

                article.get("published_utc", "")
            ),

            reverse=True
        )

    repair_queue = repair_candidates[:REPAIR_LIMIT]

    print(

        f"[INFO] Legacy translations requiring repair: "

        f"{len(repair_candidates)}"
    )

    print(

        f"[INFO] Selected repairs: "

        f"{len(repair_queue)}"
    )

    # --------------------------------------------------------
    # 6. Translate new articles first.
    # --------------------------------------------------------

    new_completed = 0
    new_flagged = 0
    repaired = 0
    repair_flagged = 0

    try:

        if pending_queue:

            new_completed, new_flagged = process_queue(

                pending_queue,

                archive,

                cache,

                repair=False
            )

        # ----------------------------------------------------
        # 7. Repair older translations within runtime budget.
        # ----------------------------------------------------

        if (

            repair_queue

            and not runtime_exceeded()
        ):

            repaired, repair_flagged = process_queue(

                repair_queue,

                archive,

                cache,

                repair=True
            )

    except Exception as exc:

        print(

            f"[ERROR] Translation stage failed: "

            f"{type(exc).__name__}: {exc}"
        )

    # --------------------------------------------------------
    # 8. Persist cache and final output.
    # --------------------------------------------------------

    save_sentence_cache(
        cache
    )

    write_output_files(

        archive,

        utcnow()
    )

    statuses = Counter(

        article.get(

            "translation_status",

            "pending"
        )

        for article in archive.values()
    )

    elapsed = time.monotonic() - RUN_START

    print("=" * 65)

    print("[OK] Czech Media Tracker Version 2 completed")

    print(
        f"[OK] New articles collected: {new_count}"
    )

    print(
        f"[OK] New validated translations: {new_completed}"
    )

    print(
        f"[OK] Legacy translations repaired: {repaired}"
    )

    print(
        f"[INFO] New translations flagged: {new_flagged}"
    )

    print(
        f"[INFO] Repairs flagged: {repair_flagged}"
    )

    print(
        f"[OK] Completed translations: {statuses['completed']}"
    )

    print(
        f"[OK] Original English articles: {statuses['not_required']}"
    )

    print(
        f"[INFO] Pending translations: {statuses['pending']}"
    )

    print(
        f"[INFO] Failed translations: {statuses['failed']}"
    )

    print(
        f"[INFO] Review required: {statuses['review_required']}"
    )

    print(
        f"[INFO] Irrelevant articles: {statuses['not_relevant']}"
    )

    print(
        f"[CACHE] Hits: {CACHE_HITS}"
    )

    print(
        f"[CACHE] Misses: {CACHE_MISSES}"
    )

    print(
        f"[CACHE] Total stored sentences: {len(cache)}"
    )

    print(
        f"[MODEL] Sentences inferred: {MODEL_SENTENCES}"
    )

    print(
        f"[OK] Total archived articles: {len(archive)}"
    )

    print(
        f"[INFO] Runtime: {elapsed:.1f} seconds"
    )

    print("=" * 65)

    return 0


if __name__ == "__main__":

    raise SystemExit(main())
