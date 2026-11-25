# -*- coding: utf-8 -*-
"""
Czech Media Tracker — Version 3

Models:
    Translation: facebook/nllb-200-distilled-1.3B
    Named entities: popelucha/robeczech-NER

Features:
    - Persistent article archive
    - Version-aware incremental processing
    - Czech named-entity recognition
    - Conservative proper noun normalization
    - Institutional and domain terminology glossary
    - Sentence-by-sentence translation
    - Length-aware inference batching
    - Persistent translation and NER caches
    - Weighted topic classification
    - Czech-specific domestic politics filtering
    - Translation validation and targeted retries
    - One-time upgrades of active legacy translations
    - Bounded retries and runtime
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

TRANSLATION_VERSION = 3
CLASSIFICATION_VERSION = 3
VALIDATION_VERSION = 3
NER_VERSION = 1

MODEL_NAME = os.getenv(
    "MODEL_NAME",
    "facebook/nllb-200-distilled-1.3B"
)

NER_MODEL_NAME = os.getenv(
    "NER_MODEL_NAME",
    "popelucha/robeczech-NER"
)

KEEP_HOURS = int(os.getenv("KEEP_HOURS", "48"))

TRANSLATE_LIMIT = int(os.getenv("TRANSLATE_LIMIT", "60"))
UPGRADE_LIMIT = int(os.getenv("UPGRADE_LIMIT", "30"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "10"))

BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "8")))
NER_BATCH_SIZE = max(1, int(os.getenv("NER_BATCH_SIZE", "16")))
ARTICLE_GROUP_SIZE = max(
    1, int(os.getenv("ARTICLE_GROUP_SIZE", "8"))
)

NUM_BEAMS = max(1, int(os.getenv("NUM_BEAMS", "4")))
CPU_THREADS = max(1, int(os.getenv("CPU_THREADS", "4")))

MAX_RUNTIME_MINUTES = int(
    os.getenv("MAX_RUNTIME_MINUTES", "40")
)

MAX_TRANSLATION_ATTEMPTS = 3

TITLE_WEIGHT = 3
LEAD_WEIGHT = 1

NER_MIN_CONFIDENCE = float(
    os.getenv("NER_MIN_CONFIDENCE", "0.85")
)

ENABLE_NER = os.getenv("ENABLE_NER", "1") == "1"

TRANSLATE_ALL = os.getenv("TRANSLATE_ALL", "0") == "1"

UPGRADE_ACTIVE_ARTICLES = (
    os.getenv("UPGRADE_ACTIVE_ARTICLES", "1") == "1"
)

UPGRADE_HISTORICAL_ARTICLES = (
    os.getenv("UPGRADE_HISTORICAL_ARTICLES", "0") == "1"
)

SOURCE_LANGUAGE = "ces_Latn"
TARGET_LANGUAGE = "eng_Latn"

MAX_INPUT_TOKENS = 512
MAX_NEW_TOKENS = 256

DATA_DIR = Path("data")

ARCHIVE_JSON = DATA_DIR / "articles_archive.json"
OUT_JSON = DATA_DIR / "articles_latest.json"

SENTENCE_CACHE_JSON = (
    DATA_DIR / "sentence_translation_cache.json"
)

NER_CACHE_JSON = DATA_DIR / "named_entity_cache.json"

TIMEOUT = 20
MAX_RETRIES = 3

RUN_START = time.monotonic()

TOKENIZER = None
MODEL = None
TORCH = None

NER_PIPELINE = None
NER_UNAVAILABLE = False

STATS = Counter()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept": (
        "application/rss+xml, application/xml, "
        "text/xml, */*;q=0.8"
    ),
}


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
# GENERAL HELPERS
# ============================================================

def utcnow():
    return datetime.now(timezone.utc)


def utc_string(dt):
    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def normalize_space(text):
    return " ".join((text or "").split()).strip()


def clean_text(text):
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize_space(text)


def normalize_url(url):
    url = normalize_space(url)
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)
    return url.rstrip("/")


def normalize_title(text):
    return re.sub(
        r"[^\w\s]",
        "",
        normalize_space(text).casefold()
    )


def fold_accents(text):
    return "".join(
        c for c in unicodedata.normalize("NFKD", text or "")
        if not unicodedata.combining(c)
    ).casefold()


def hash_key(*parts):
    h = hashlib.sha1()

    for part in parts:
        h.update(str(part or "").encode("utf-8"))
        h.update(b"|")

    return h.hexdigest()


def article_fingerprint(article):

    source = article.get("source", "")
    url = normalize_url(article.get("url", ""))

    if url:
        return hash_key(source, url)

    return hash_key(
        source,
        normalize_title(article.get("title_raw", "")),
        article.get("published_utc", "")[:10],
    )


def within_window(article, now):

    try:
        published = datetime.fromisoformat(
            article["published_utc"].replace("Z", "+00:00")
        )

        return (
            now - timedelta(hours=KEEP_HOURS)
            <= published
            <= now + timedelta(minutes=10)
        )

    except (ValueError, KeyError, TypeError):
        return False


def runtime_exceeded():

    return (
        time.monotonic() - RUN_START
        >= MAX_RUNTIME_MINUTES * 60
    )


def atomic_write_json(path, payload):

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temporary.replace(path)


def load_json(path, default=None):

    if not path.exists():
        return default

    return json.loads(
        path.read_text(encoding="utf-8")
    )


# ============================================================
# SENTENCE SEGMENTATION
# ============================================================

ABBREVIATIONS = [
    "např.", "tj.", "tzn.", "tzv.",
    "apod.", "atd.", "resp.", "pozn.",
    "č.", "odst.", "str.", "sb.",
    "mil.", "mld.", "tis.",
    "ing.", "mgr.", "mudr.",
    "judr.", "phdr.", "rndr.",
    "prof.", "doc.", "st.", "sv.",
    "a.s.", "s.r.o.",
    "U.S.", "U.K.",
]

MONTHS = (
    "ledna|února|března|dubna|května|června|"
    "července|srpna|září|října|listopadu|prosince"
)


def split_sentences(text):

    text = normalize_space(text)

    if not text:
        return []

    marker = "\uE000"

    protected = re.sub(
        r"(?<=\d)\.(?=\d)",
        marker,
        text,
    )

    protected = re.sub(
        rf"(?<=\d)\.(?=\s+(?:{MONTHS})\b)",
        marker,
        protected,
        flags=re.IGNORECASE,
    )

    for abbreviation in sorted(
        ABBREVIATIONS,
        key=len,
        reverse=True,
    ):

        protected = re.sub(
            r"(?<!\w)" + re.escape(abbreviation),
            lambda m: m.group(0).replace(".", marker),
            protected,
            flags=re.IGNORECASE,
        )

    protected = re.sub(
        r"\b([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ])\."
        r"(?=\s+[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ])",
        lambda m: m.group(1) + marker,
        protected,
    )

    parts = re.split(
        r"""(?<=[.!?])\s+|(?<=[.!?]["”’»‘“])\s+""",
        protected,
    )

    return [
        normalize_space(part.replace(marker, "."))
        for part in parts
        if part.strip()
    ]


def two_sentence_lead(text):

    return " ".join(
        split_sentences(clean_text(text))[:2]
    )


def join_sentences(translations):

    output = []

    for i, translation in enumerate(translations):

        text = normalize_space(translation)

        if not text:
            continue

        if i < len(translations) - 1:

            if not re.search(
                r"""[.!?]["”’»‘“]?$""",
                text,
            ):
                text += "."

        output.append(text)

    return " ".join(output)


# ============================================================
# RSS COLLECTION
# ============================================================

def fetch_text(url):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT,
            )

            response.raise_for_status()

            if response.text:
                return response.text

        except requests.RequestException as exc:

            print(
                f"[WARN] RSS attempt {attempt}: {exc}"
            )

        if attempt < MAX_RETRIES:
            time.sleep(attempt * 2)

    return None


def parse_entry_datetime(entry):

    for key in (
        "published_parsed",
        "updated_parsed",
        "created_parsed",
    ):

        value = entry.get(key)

        if value:
            try:
                return datetime(
                    *value[:6],
                    tzinfo=timezone.utc,
                )
            except Exception:
                pass

    for key in (
        "published",
        "updated",
        "created",
        "pubDate",
    ):

        value = entry.get(key)

        if value:
            try:
                return pd.to_datetime(
                    value,
                    utc=True,
                    errors="raise",
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

            title = clean_text(entry.get("title", ""))

            if not title:
                continue

            published = parse_entry_datetime(entry)

            if published is None:
                continue

            row = {
                "source": source_name,
                "source_language": config["language"],
                "url": normalize_url(
                    entry.get("link", "")
                ),
                "published_utc": utc_string(published),
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
# WEIGHTED CLASSIFICATION
#
# Each concept has:
#   weight
#   Czech regex
#   English regex
#
# Concepts are scored only once, preventing Czech and English
# translations from artificially doubling keyword scores.
# ============================================================

TOPIC_RULES = {

    "diplomacy": {

        "ambassador": (
            3,
            r"\bvelvyslan\w*|\bambasád\w*",
            r"\bambassadors?\b|\bembass(?:y|ies)\b",
        ),

        "foreign_policy": (
            3,
            r"\bzahraniční politik\w*|"
            r"\bministerstv\w* zahraničních věcí",
            r"\bforeign polic\w*|"
            r"\bforeign ministr\w*",
        ),

        "summit": (
            3,
            r"\bsummit\w*",
            r"\bsummits?\b",
        ),

        "diplomatic_relations": (
            3,
            r"\bdiplomac\w*|\bdiplomatick\w*",
            r"\bdiplomac\w*|\bdiplomatic\b",
        ),

        "peace_negotiations": (
            3,
            r"\bmírov\w* jednán\w*|"
            r"\bmírov\w* rozhovor\w*",
            r"\bpeace talks\b|"
            r"\bpeace negotiations\b",
        ),

        "bilateral": (
            2,
            r"\bbilaterál\w*|\bdvoustrann\w*",
            r"\bbilateral\b|\bmultilateral\b",
        ),

        "international_institutions": (
            2,
            r"\bEvropsk\w* rad\w*|\bOSN\b",
            r"\bEuropean Council\b|"
            r"\bUnited Nations\b",
        ),

        "state_visit": (
            3,
            r"\bstátní návštěv\w*",
            r"\bstate visits?\b",
        ),
    },

    "military": {

        "armed_forces": (
            3,
            r"\barmád\w*|\bvoják\w*|"
            r"\bvojác\w*|\bvojensk\w*",
            r"\bmilitary\b|\barmed forces\b|"
            r"\bsoldiers?\b|\btroops\b",
        ),

        "war": (
            3,
            r"\bválk\w*|\bváleč\w*",
            r"\bwar\b|\bwarfare\b",
        ),

        "weapons": (
            3,
            r"\bzbran\w*|\bzbraň\w*|"
            r"\bmunic\w*|\bvýzbroj\w*",
            r"\bweapons?\b|\bammunition\b|"
            r"\bartillery\b",
        ),

        "missiles": (
            3,
            r"\braket\w*|\bstřel\w* dlouhého doletu",
            r"\bmissiles?\b|\brockets?\b",
        ),

        "defense_industry": (
            3,
            r"\bzbrojov\w*|"
            r"\bobrann\w* průmysl\w*",
            r"\bdefen[cs]e industry\b",
        ),

        "defense_spending": (
            3,
            r"\bvýdaj\w* na obranu|"
            r"\brozpočt\w* na obranu",
            r"\bdefen[cs]e spending\b|"
            r"\bdefen[cs]e budget\b",
        ),

        "nato": (
            3,
            r"\bNATO\b",
            r"\bNATO\b",
        ),

        "intelligence": (
            3,
            r"\bBIS\b|"
            r"\bzpravodajsk\w* služb\w*|"
            r"\bvojenské zpravodajství\b",
            r"\bintelligence service\w*|"
            r"\bmilitary intelligence\b",
        ),

        "conflict_attacks": (
            3,
            r"\b(?:rusk\w*|ukrajinsk\w*|"
            r"izraelsk\w*)\s+útok\w*|"
            r"\b(?:Ukrajinci|Rusové)\s+zaútoč\w*",
            r"\b(?:Russian|Ukrainian|Israeli)"
            r"\s+attacks?\b",
        ),

        "combat": (
            3,
            r"\bbojov\w*|\bbojišt\w*|"
            r"\bdělostřel\w*",
            r"\bcombat\b|\bbattlefield\b",
        ),

        "air_defense": (
            3,
            r"\bprotivzdušn\w*|\bsestřel\w*",
            r"\bair defen[cs]e\b|"
            r"\bshooting down missiles\b",
        ),

        "hybrid_warfare": (
            3,
            r"\bhybridní (?:válk|hrozb)\w*",
            r"\bhybrid warfare\b|"
            r"\bhybrid threats?\b",
        ),

        "cybersecurity": (
            3,
            r"\bkybernetick\w* útok\w*|"
            r"\bkyberbezpečnost\w*",
            r"\bcyberattacks?\b|"
            r"\bcybersecurity\b",
        ),

        "military_drones": (
            3,
            r"\bbojov\w* dron\w*|"
            r"\bvojensk\w* dron\w*|"
            r"\bbezpilotn\w* letoun\w*",
            r"\bmilitary drones?\b|"
            r"\bcombat drones?\b",
        ),
    },

    "energy": {

        "nuclear": (
            3,
            r"\bjadern\w*|\btemelín\w*|"
            r"\bdukovan\w*",
            r"\bnuclear\b|\bTemelin\b|"
            r"\bDukovany\b",
        ),

        "electricity": (
            3,
            r"\belektřin\w*|\belektrárn\w*",
            r"\belectricity\b|\bpower plants?\b",
        ),

        "district_heating": (
            3,
            r"\bteplár\w*|\bcen\w* tepla",
            r"\bdistrict heating\b|"
            r"\bheating prices?\b",
        ),

        "oil_gas": (
            3,
            r"\bropovod\w*|\bropn\w*|"
            r"\bplynovod\w*|\brafineri\w*",
            r"\boil pipelines?\b|"
            r"\bnatural gas\b|"
            r"\brefiner(?:y|ies)\b",
        ),

        "renewables": (
            3,
            r"\bfotovolta\w*|"
            r"\bv[ěe]trn\w* elektrárn\w*|"
            r"\bobnoviteln\w* zdroj\w*",
            r"\brenewable energy\b|"
            r"\bsolar power\b|\bwind farms?\b",
        ),

        "energy_policy": (
            3,
            r"\benergetick\w* kriz\w*|"
            r"\benergetick\w* bezpečnost\w*|"
            r"\benergetick\w* politik\w*",
            r"\benergy crisis\b|"
            r"\benergy security\b|"
            r"\benergy policy\b",
        ),

        "cez": (
            3,
            r"\bČEZ\b",
            r"\bCEZ\b|\bČEZ\b",
        ),

        "grid": (
            3,
            r"\bpřenosov\w* soustav\w*|"
            r"\belektrick\w* sít\w*",
            r"\bpower grid\b|"
            r"\belectricity grid\b",
        ),

        "generic_energy": (
            1,
            r"\benergi\w*|\benerget\w*",
            r"\benergy\b",
        ),

        "fuel": (
            2,
            r"\bpaliv\w*|\buhlí\b",
            r"\bcoal\b|\bfuel\b",
        ),
    },

    "economy": {

        "inflation": (
            3,
            r"\binflac\w*",
            r"\binflation\b",
        ),

        "gdp": (
            3,
            r"\bHDP\b",
            r"\bGDP\b",
        ),

        "central_bank": (
            3,
            r"\bČNB\b|"
            r"\bcentrální bank\w*",
            r"\bcentral bank\b|"
            r"\bCzech National Bank\b",
        ),

        "interest_rates": (
            3,
            r"\búrokov\w* sazb\w*",
            r"\binterest rates?\b",
        ),

        "wages": (
            3,
            r"\bmzd\w*|\bplatov\w*",
            r"\bwages?\b|\bsalaries\b",
        ),

        "unemployment": (
            3,
            r"\bnezaměstnan\w*",
            r"\bunemployment\b",
        ),

        "pensions": (
            3,
            r"\bdůchod\w*",
            r"\bpensions?\b",
        ),

        "tariffs": (
            3,
            r"\b(?:clo|cla|cel|clům|cly)\b",
            r"\btariffs?\b",
        ),

        "trade": (
            3,
            r"\bexport\w*|\bvývoz\w*|"
            r"\bdovoz\w*|\bimport\w*",
            r"\bimports?\b|\bexports?\b|"
            r"\btrade agreements?\b",
        ),

        "government_budget": (
            3,
            r"\bstátní\w* rozpočt\w*|"
            r"\bschodk\w*|"
            r"\bveřejn\w* financ\w*",
            r"\bstate budget\b|"
            r"\bpublic finances\b|"
            r"\bbudget deficit\b",
        ),

        "economic_policy": (
            3,
            r"\bfiskáln\w*|\bměnov\w* politik\w*",
            r"\bfiscal policy\b|"
            r"\bmonetary policy\b",
        ),

        "subsidies": (
            3,
            r"\bdotac\w*|\bdotační\w*",
            r"\bsubsid(?:y|ies)\b",
        ),

        "taxes": (
            3,
            r"\bdaň\w*|\bdaní\b|"
            r"\bdaňov\w*",
            r"\btax(?:es|ation)?\b",
        ),

        "stock_market": (
            3,
            r"\bburz\w*|\bakciov\w* trh\w*",
            r"\bstock market\b|"
            r"\bstock exchange\b",
        ),

        "mortgages": (
            3,
            r"\bhypoték\w*",
            r"\bmortgages?\b",
        ),

        "industrial_policy": (
            3,
            r"\bprůmyslov\w* politik\w*|"
            r"\bprůmyslov\w* výrob\w*",
            r"\bindustrial policy\b|"
            r"\bindustrial production\b",
        ),

        "investment": (
            2,
            r"\binvestic\w*",
            r"\binvestments?\b",
        ),

        "trade_unions": (
            3,
            r"\b(?:odbory|odborů|odborům|odborech)\b|"
            r"\bodborov\w* svaz\w*",
            r"\btrade unions?\b|"
            r"\blabou?r unions?\b",
        ),

        "economic_growth": (
            3,
            r"\bekonomick\w* růst\w*|"
            r"\bhospodářsk\w* růst\w*",
            r"\beconomic growth\b",
        ),

        "generic_economy": (
            2,
            r"\bekonom\w*",
            r"\beconom(?:y|ic|ics)\b",
        ),

        "generic_budget": (
            1,
            r"\brozpočt\w*",
            r"\bbudgets?\b",
        ),

        "currency": (
            1,
            r"\bkorun\w*|\bKč\b",
            r"\bCZK\b|\bCzech crowns\b",
        ),

        "employment": (
            1,
            r"\bzaměstnan\w*|\bzaměstnavatel\w*",
            r"\bemployees?\b|\bemployment\b",
        ),

        "prices": (
            1,
            r"\bcenov\w*|\bcen[ay]\b",
            r"\bprices?\b",
        ),
    },

    "local_events": {

        "fire": (
            3,
            r"\bpožár\w*|\bhasič\w*",
            r"\bfirefighters?\b|\bfires?\b",
        ),

        "natural_disaster": (
            3,
            r"\bpovod\w*|\bzáplav\w*|"
            r"\bzemětřesen\w*|\bsesuv\w*",
            r"\bfloods?\b|\bearthquakes?\b|"
            r"\blandslides?\b",
        ),

        "severe_weather": (
            3,
            r"\bvichřic\w*|"
            r"\bsiln\w* bouř\w*",
            r"\bsevere storms?\b",
        ),

        "accident": (
            3,
            r"\bdopravn\w* nehod\w*|"
            r"\bželezniční nehod\w*",
            r"\btraffic accidents?\b|"
            r"\btrain crashes?\b",
        ),

        "evacuation": (
            3,
            r"\bevakuac\w*",
            r"\bevacuat\w*",
        ),

        "infrastructure_failure": (
            3,
            r"\bvýpad\w* elektřin\w*|"
            r"\bzřícen\w* most\w*",
            r"\bpower outages?\b|"
            r"\bbridge collapse\b",
        ),

        "emergency": (
            2,
            r"\bmimořádn\w* událost\w*",
            r"\bemergency services\b",
        ),
    },

    "domestic_politics": {

        "parliament": (
            3,
            r"\bsněmovn\w*|\bposlanc\w*|"
            r"\bposlanec\b",
            r"\bChamber of Deputies\b|"
            r"\bMembers of Parliament\b",
        ),

        "government": (
            2,
            r"\bvlád\w*|\bministerstv\w*",
            r"\bCzech government\b|"
            r"\bgovernment ministr\w*",
        ),

        "elections": (
            3,
            r"\bvolb\w*|\bvoleb\w*|"
            r"\bvolič\w*",
            r"\belections?\b",
        ),

        "coalition": (
            3,
            r"\bkoalic\w*|\bopozic\w*",
            r"\bcoalition\b|\bopposition\b",
        ),

        "political_parties": (
            3,
            r"\b(?:ANO|ODS|SPD|STAN|TOP 09|KDU-ČSL)\b",
            r"\b(?:ANO|ODS|SPD|STAN|TOP 09|KDU-ČSL)\b",
        ),

        "constitutional": (
            3,
            r"\bústavn\w* soud\w*|"
            r"\bústavn\w* zákon\w*",
            r"\bconstitutional court\b|"
            r"\bconstitutional law\b",
        ),

        "municipal_government": (
            3,
            r"\bstarost\w*|\bhejtman\w*|"
            r"\bzastupitel\w*|\breferend\w*",
            r"\bmayors?\b|\bregional governors?\b|"
            r"\bmunicipal councils?\b",
        ),

        "legislation": (
            3,
            r"\bnávrh\w* zákon\w*|"
            r"\bnovel\w* zákon\w*",
            r"\blegislation\b|\bdraft law\b",
        ),

        "leadership": (
            1,
            r"\bpremiér\w*|\bprezident\w*|"
            r"\bministr\w*",
            r"\bprime minister\b|"
            r"\bpresident\b|\bminister\b",
        ),
    },

    "foreign_affairs": {

        "international_conflict": (
            3,
            r"\bmezinárodn\w* konflikt\w*|"
            r"\bgeopolit\w*",
            r"\binternational conflict\b|"
            r"\bgeopolitic\w*",
        ),

        "sanctions": (
            3,
            r"\bsankc\w*",
            r"\bsanctions?\b",
        ),

        "international_politics": (
            2,
            r"\bzahraničn\w* politik\w*|"
            r"\bmezinárodn\w* vztah\w*",
            r"\bforeign policy\b|"
            r"\binternational relations\b",
        ),

        "eu_policy": (
            3,
            r"\bEvropsk\w* komis\w*|"
            r"\bEvropsk\w* parlament\w*|"
            r"\beuroposlanc\w*",
            r"\bEuropean Commission\b|"
            r"\bEuropean Parliament\b|\bMEPs\b",
        ),

        "international_security": (
            3,
            r"\bmezinárodn\w* bezpečnost\w*",
            r"\binternational security\b",
        ),

        "foreign_government": (
            2,
            r"\b(?:americk|rusk|čínsk|slovensk|"
            r"německ|francouzsk|britsk|ukrajinsk)"
            r"\w*\s+(?:vlád|prezident|premiér|ministr)\w*",
            r"\b(?:American|Russian|Chinese|Slovak|"
            r"German|French|British|Ukrainian)"
            r"\s+(?:government|president|prime minister|minister)\b",
        ),

        "strategic_competition": (
            3,
            r"\bstrategick\w* soupeřen\w*",
            r"\bstrategic competition\b",
        ),
    },
}


TOPIC_THRESHOLDS = {
    "diplomacy": 6,
    "military": 6,
    "energy": 6,
    "economy": 7,
    "local_events": 6,
    "domestic_politics": 6,
    "foreign_affairs": 6,
}


CZECH_CONTEXT = (
    r"\bČesk\w*|\bČesko\b|\bČR\b|"
    r"\bPra[hz]\w*|\bCzech\b|"
    r"\bBabiš\w*|\bFial\w*|"
    r"\bOkamur\w*|\bSchillerov\w*|"
    r"\bČernochov\w*|"
    r"\bPetr Pavel\b|"
    r"\bSněmovn\w*|"
    r"\bChamber of Deputies\b|"
    r"\b(?:ANO|ODS|STAN|SPD|KDU-ČSL)\b"
)

FOREIGN_CONTEXT = (
    r"\bslovensk\w*|\brusk\w*|"
    r"\bčínsk\w*|\bamerick\w*|"
    r"\bukrajinsk\w*|\bněmeck\w*|"
    r"\bfrancouzsk\w*|\bbritsk\w*|"
    r"\bSlovak\b|\bRussian\b|\bChinese\b|"
    r"\bAmerican\b|\bUkrainian\b"
)


def regex_match(pattern, text):

    return bool(
        re.search(pattern, text or "", re.IGNORECASE)
    )


def concept_matches(pattern, text):

    # Preserve exact case for short political acronyms.
    # This prevents ordinary Czech "ano" from matching ANO.

    if "(?:ANO|ODS" in pattern:

        return bool(re.search(pattern, text or ""))

    return regex_match(pattern, text)


def article_has_czech_context(article):

    raw = (
        f"{article.get('title_raw') or ''} "
        f"{article.get('lead_raw') or ''}"
    )

    if regex_match(CZECH_CONTEXT, raw):
        return True

    url = article.get("url", "").lower()
    source = article.get("source", "")

    domestic_paths = (
        "/domaci/",
        "/domaci-",
        "/zpravy-domov/",
        "/ceske-budejovice/",
        "/praha/",
    )

    if any(path in url for path in domestic_paths):
        return True

    if "Domestic" in source:
        return True

    if regex_match(FOREIGN_CONTEXT, raw):
        return False

    return False


def classify_topics(article):

    language = article.get("source_language", "cs")

    title = article.get("title_raw") or ""
    lead = article.get("lead_raw") or ""

    scores = {}
    matched = {}
    details = {}
    topics = []

    for topic, concepts in TOPIC_RULES.items():

        score = 0
        strong_match = False

        topic_matches = []
        topic_details = []

        for concept, rule in concepts.items():

            strength, cs_pattern, en_pattern = rule

            pattern = (
                en_pattern if language == "en"
                else cs_pattern
            )

            title_hit = concept_matches(
                pattern,
                title,
            )

            lead_hit = concept_matches(
                pattern,
                lead,
            )

            if not title_hit and not lead_hit:
                continue

            # A concept is counted only once, using the
            # strongest location in which it appears.

            location_weight = (
                TITLE_WEIGHT if title_hit
                else LEAD_WEIGHT
            )

            points = strength * location_weight

            score += points

            if strength >= 3:
                strong_match = True

            topic_matches.append(concept)

            topic_details.append({
                "concept": concept,
                "strength": strength,
                "headline": title_hit,
                "lead": lead_hit,
                "points": points,
            })

        qualifies = (
            score >= TOPIC_THRESHOLDS[topic]
            and strong_match
        )

        if topic in (
            "domestic_politics",
            "local_events",
        ):

            if not article_has_czech_context(article):
                qualifies = False

        scores[topic] = score
        matched[topic] = topic_matches
        details[topic] = topic_details

        if qualifies:
            topics.append(topic)

    article["topic_scores"] = scores
    article["matched_keywords"] = matched
    article["classification_details"] = details

    article["topics"] = topics
    article["relevant"] = bool(topics)

    article["primary_topic"] = (
        max(topics, key=lambda topic: scores[topic])
        if topics else None
    )

    article["classification_version"] = (
        CLASSIFICATION_VERSION
    )

    STATS["classified_articles"] += 1

    return article


# ============================================================
# INSTITUTIONAL AND DOMAIN GLOSSARY
#
# Names are normalized conservatively.
# Context-dependent vocabulary is matched through phrases
# rather than unrestricted replacements wherever possible.
# ============================================================

GLOSSARY = [

    # Political figures

    (
        r"\bAndrej(?:e|ovi|em)? "
        r"Babiš(?:e|ovi|em)?\b",
        "Andrej Babiš",
    ),

    (
        r"\bBabiš(?:e|ovi|em|ův|ova|ovo)?\b",
        "Babiš",
    ),

    (
        r"\bPetr(?:a|ovi|em)? "
        r"Pav(?:el|la|lovi|lem)\b",
        "Petr Pavel",
    ),

    (
        r"\bPav(?:el|la|lovi|lem)\b",
        "Pavel",
    ),

    (
        r"\bSchillerov(?:á|é|ou|ě|y)\b",
        "Schillerová",
    ),

    (
        r"\bČernochov(?:á|é|ou|ě|y)\b",
        "Černochová",
    ),

    (
        r"\bKopečn(?:ý|ého|ému|ým)\b",
        "Kopečný",
    ),

    (
        r"\bOkamur(?:a|y|ovi|ou|ova|ovo)\b",
        "Okamura",
    ),

    (
        r"\bTejc(?:e|ovi|em)?\b",
        "Tejc",
    ),

    (
        r"\bMlsn(?:a|y|ovi|ou)\b",
        "Mlsna",
    ),

    (
        r"\bFial(?:a|y|ovi|ou)\b",
        "Fiala",
    ),

    (
        r"\bFic(?:o|a|ovi|em)\b",
        "Fico",
    ),

    (
        r"\bHavlíčk(?:a|ovi|em)\b",
        "Havlíček",
    ),

    (
        r"\bJuchelk(?:a|y|ovi|ou)\b",
        "Juchelka",
    ),

    (
        r"\bStředul(?:a|y|ovi|ou)\b",
        "Středula",
    ),

    (
        r"\bLipavsk(?:ý|ého|ému|ým)\b",
        "Lipavský",
    ),

    # Institutions

    (
        r"\bÚOHS\b",
        "Office for the Protection of Competition",
    ),

    (
        r"\bKRNAP(?:u|em)?\b",
        "Krkonoše National Park",
    ),

    (
        r"\bČNB\b",
        "Czech National Bank",
    ),

    (
        r"\bMPSV\b",
        "Ministry of Labour and Social Affairs",
    ),

    (
        r"\bMPO\b",
        "Ministry of Industry and Trade",
    ),

    (
        r"\bMZV\b",
        "Ministry of Foreign Affairs",
    ),

    (
        r"\bMŠMT\b",
        "Ministry of Education, Youth and Sports",
    ),

    (
        r"\bČRo\b",
        "Czech Radio",
    ),

    (
        r"\bČT\b",
        "Czech Television",
    ),

    (
        r"\bČTK\b",
        "Czech News Agency",
    ),

    (
        r"\bČEZ\b",
        "ČEZ",
    ),

    (
        r"\bBIS\b",
        "Security Information Service",
    ),

    (
        r"\bČR\b",
        "Czech Republic",
    ),

    (
        r"\bPoslaneck(?:á|é|ou) "
        r"sněmovn(?:a|y|ě|u|ou)\b",
        "Chamber of Deputies",
    ),

    (
        r"\bSněmovn(?:a|y|ě|u|ou)\b",
        "Chamber of Deputies",
    ),

    (
        r"\bTeplárensk(?:é|ého|ému|ým) "
        r"sdružen(?:í|ím)\b",
        "District Heating Association",
    ),

    # Czech locations

    (
        r"\bVelk(?:é|ého|ém|ým) Meziříčí\b",
        "Velké Meziříčí",
    ),

    (
        r"\b(?:Dvůr|Dvora|Dvoře|Dvorem) "
        r"Králové nad Labem\b",
        "Dvůr Králové nad Labem",
    ),

    (
        r"\bNov(?:ý|ého|ém|ým) Jičín(?:a|ě|em)?\b",
        "Nový Jičín",
    ),

    (
        r"\bMariánsk(?:é|ých|ým) "
        r"Lázn(?:ě|í|ích|ěmi)\b",
        "Mariánské Lázně",
    ),

    (
        r"\bČesk(?:é|ých|ým) "
        r"Budějovic(?:e|ích|ím|emi)?\b",
        "České Budějovice",
    ),

    (
        r"\bDukovan(?:y|ech|ům|ami)?\b",
        "Dukovany",
    ),

    (
        r"\bTemelín(?:a|ě|em|u)?\b",
        "Temelín",
    ),

    (
        r"\bPřerovsk(?:o|a|u|em)\b",
        "Přerov region",
    ),

    (
        r"\bBruntálsk(?:o|a|u|em)\b",
        "Bruntál region",
    ),

    (
        r"\bAlžbětin(?:y|ých|ým) "
        r"lázn(?:ě|í|ích)\b",
        "Alžbětiny Lázně",
    ),

    (
        r"\bBila Cerkva\b",
        "Bila Tserkva",
    ),

    # Currency

    (
        r"\bKč\b",
        "CZK",
    ),

    (
        r"\bkorun českých\b",
        "Czech crowns",
    ),

    # Technical terminology

    (
        r"\bdřevomork(?:a|y|ou|u)\b",
        "dry rot fungus",
    ),

    (
        r"\bčističk(?:a|y|u|ou) "
        r"odpadních vod\b",
        "wastewater treatment plant",
    ),

    (
        r"\bsestřelování raket\b",
        "shooting down missiles",
    ),

    (
        r"\bza kubík\b",
        "per cubic meter",
    ),

    (
        r"\bza metr krychlový\b",
        "per cubic meter",
    ),

    (
        r"\bteplárny\b",
        "district heating plants",
    ),

    (
        r"\bteplárna\b",
        "district heating plant",
    ),

    (
        r"\bjednomyslnost(?:i|í)?\b",
        "unanimity",
    ),

    (
        r"\bpředčasn(?:ý|é|ého|ému) důchod\b",
        "early retirement pension",
    ),

    (
        r"\bodvolání z funkce\b",
        "removal from office",
    ),

    (
        r"\bve vazbě\b",
        "in pretrial detention",
    ),

    (
        r"\bdo vazby\b",
        "into pretrial detention",
    ),

    (
        r"\bhejtmani\b",
        "regional governors",
    ),

    (
        r"\bhejtman\b",
        "regional governor",
    ),

    (
        r"(?<=ŽIVĚ: )TK\b",
        "press conference",
    ),
]


# ============================================================
# KNOWN SEMANTIC FAILURE PATTERNS
# ============================================================

SEMANTIC_CHECKS = [

    (
        r"\bÚOHS\b",
        r"\bCIA\b",
        "incorrect_institution",
    ),

    (
        r"\bBabiš",
        r"\bgrandma\b|\bgrandmother\b",
        "incorrect_person_name",
    ),

    (
        r"\bKopečn",
        r"\bhiccup",
        "incorrect_person_name",
    ),

    (
        r"\bČernochov",
        r"\bblackhead\b",
        "incorrect_person_name",
    ),

    (
        r"\bTejc",
        r"\byour uncle\b",
        "incorrect_person_name",
    ),

    (
        r"\bMeziříčí\b",
        r"\bGrand Canyon\b",
        "incorrect_location",
    ),

    (
        r"Králové nad Labem",
        r"Palace of the Kings",
        "incorrect_location",
    ),

    (
        r"\bdřevomork",
        r"\bwoodpecker\b",
        "incorrect_technical_term",
    ),

    (
        r"\bteplár",
        r"\bgreenhouses?\b|\bboilers\b",
        "incorrect_energy_term",
    ),

    (
        r"\bsestřelov",
        r"\bmissile strikes?\b",
        "incorrect_military_action",
    ),

    (
        r"\bKč\b|\bkorun",
        r"\bcents\b|\bdollars\b",
        "possible_currency_error",
    ),

    (
        r"\bodvolání",
        r"\bresignation\b",
        "possible_removal_resignation_error",
    ),

    (
        r"\bpadlých\b|\bpadlí\b",
        r"\bcasualties\b",
        "fatalities_translated_as_casualties",
    ),

    (
        r"\bposlanc\w*|\bposlanec\b",
        r"\bMEPs?\b",
        "possible_parliament_error",
    ),
]


# ============================================================
# PERSISTENT CACHES
# ============================================================

def load_cache(path):

    try:

        data = load_json(path, {})

        if isinstance(data, dict):

            return (
                data.get("entries")
                or data.get("translations")
                or {}
            )

    except Exception as exc:

        print(
            f"[WARN] Cache could not be read: "
            f"{path}: {exc}"
        )

    return {}


def save_caches(sentence_cache, entity_cache):

    atomic_write_json(
        SENTENCE_CACHE_JSON,
        {
            "translation_version": TRANSLATION_VERSION,
            "validation_version": VALIDATION_VERSION,
            "model": MODEL_NAME,
            "updated_at_utc": utc_string(utcnow()),
            "count": len(sentence_cache),
            "entries": sentence_cache,
        },
    )

    atomic_write_json(
        NER_CACHE_JSON,
        {
            "ner_version": NER_VERSION,
            "model": NER_MODEL_NAME,
            "updated_at_utc": utc_string(utcnow()),
            "count": len(entity_cache),
            "entries": entity_cache,
        },
    )


def sentence_cache_key(text, entities):

    entity_signature = json.dumps(
        entities,
        ensure_ascii=False,
        sort_keys=True,
    )

    return hash_key(
        MODEL_NAME,
        TRANSLATION_VERSION,
        VALIDATION_VERSION,
        NER_VERSION,
        NUM_BEAMS,
        normalize_space(text),
        entity_signature,
    )


def entity_cache_key(text):

    return hash_key(
        NER_MODEL_NAME,
        NER_VERSION,
        NER_MIN_CONFIDENCE,
        normalize_space(text),
    )


# ============================================================
# CZECH NAMED-ENTITY RECOGNITION
# ============================================================

def load_ner():

    global NER_PIPELINE
    global NER_UNAVAILABLE

    if not ENABLE_NER or NER_UNAVAILABLE:
        return None

    if NER_PIPELINE is not None:
        return NER_PIPELINE

    try:

        import torch

        from transformers import (
            AutoTokenizer,
            AutoModelForTokenClassification,
            pipeline,
        )

        torch.set_num_threads(
            min(CPU_THREADS, os.cpu_count() or CPU_THREADS)
        )

        started = time.monotonic()

        print(f"[NER] Loading {NER_MODEL_NAME}")

        tokenizer = AutoTokenizer.from_pretrained(
            NER_MODEL_NAME,
            use_fast=True,
        )

        model = (
            AutoModelForTokenClassification.from_pretrained(
                NER_MODEL_NAME
            )
        )

        model.eval()

        NER_PIPELINE = pipeline(
            "token-classification",
            model=model,
            tokenizer=tokenizer,
            aggregation_strategy="simple",
            device=-1,
        )

        print(
            "[NER] Labels:",
            model.config.id2label,
        )

        print(
            f"[NER] Loaded in "
            f"{time.monotonic() - started:.1f}s"
        )

        return NER_PIPELINE

    except Exception as exc:

        NER_UNAVAILABLE = True

        print(
            f"[WARN] NER unavailable: {exc}"
        )

        print(
            "[WARN] Falling back to glossary-only protection."
        )

        return None


def is_supported_entity_label(label):

    label = str(label).upper()

    label = re.sub(
        r"^[BI](-|_)",
        "",
        label,
    )

    supported = {
        "PER", "PERSON",
        "ORG", "ORGANIZATION",
        "LOC", "LOCATION",
        "GPE",
        "P", "G", "I",
        "PF", "PS", "PC", "PM",
        "GU", "GS", "GC", "GR", "GT",
        "IF", "IO", "IC",
    }

    return label in supported


def normalize_entity_name(text):

    text = normalize_space(text)

    # Apply canonical forms only where an explicit
    # glossary mapping is available.

    for pattern, replacement in GLOSSARY:

        if re.fullmatch(pattern, text):
            return replacement

    # Unknown names are preserved exactly rather than
    # guessing how to reverse Czech inflection.

    return text


def detect_entities(texts, entity_cache):

    output = {}

    missing = []

    for text in dict.fromkeys(texts):

        key = entity_cache_key(text)

        if key in entity_cache:

            output[text] = entity_cache[key]

            STATS["ner_cache_hits"] += 1

        else:

            missing.append(text)

            STATS["ner_cache_misses"] += 1

    if not missing:
        return output

    ner = load_ner()

    if ner is None:

        # Do not permanently cache failures as if
        # they were successful empty entity results.

        for text in missing:
            output[text] = []

        return output

    for start in range(
        0, len(missing), NER_BATCH_SIZE
    ):

        batch = missing[
            start:start + NER_BATCH_SIZE
        ]

        started = time.monotonic()

        try:

            predictions = ner(
                batch,
                batch_size=NER_BATCH_SIZE,
            )

        except Exception as exc:

            print(
                f"[WARN] NER batch failed: {exc}"
            )

            for text in batch:
                output[text] = []

            continue

        for text, entities in zip(
            batch,
            predictions,
        ):

            accepted = []

            for entity in entities:

                label = entity.get(
                    "entity_group",
                    entity.get("entity", ""),
                )

                confidence = float(
                    entity.get("score", 0)
                )

                if confidence < NER_MIN_CONFIDENCE:
                    continue

                if not is_supported_entity_label(label):
                    continue

                begin = entity.get("start")
                end = entity.get("end")

                if begin is None or end is None:
                    continue

                surface = text[begin:end]

                if not surface.strip():
                    continue

                accepted.append({
                    "start": begin,
                    "end": end,
                    "text": surface,
                    "canonical": normalize_entity_name(
                        surface
                    ),
                    "type": str(label),
                    "confidence": round(
                        confidence, 4
                    ),
                })

            accepted.sort(
                key=lambda entity: entity["start"]
            )

            output[text] = accepted

            entity_cache[
                entity_cache_key(text)
            ] = accepted

            STATS["entities_detected"] += len(
                accepted
            )

            STATS["ner_sentences"] += 1

        print(
            f"[NER BATCH] {len(batch)} sentences | "
            f"{time.monotonic() - started:.1f}s"
        )

    return output


# ============================================================
# ENTITY AND TERMINOLOGY PROTECTION
# ============================================================

def glossary_spans(text):

    spans = []

    for pattern, replacement in GLOSSARY:

        for match in re.finditer(pattern, text):

            spans.append({
                "start": match.start(),
                "end": match.end(),
                "text": match.group(0),
                "canonical": replacement,
                "type": "GLOSSARY",
                "priority": 2,
            })

    return spans


def protection_spans(text, entities):

    candidates = glossary_spans(text)

    for entity in entities:

        candidates.append({
            **entity,
            "priority": 1,
        })

    candidates.sort(
        key=lambda item: (
            item["priority"],
            item["end"] - item["start"],
        ),
        reverse=True,
    )

    selected = []

    for candidate in candidates:

        overlaps = any(
            candidate["start"] < existing["end"]
            and candidate["end"] > existing["start"]
            for existing in selected
        )

        if not overlaps:
            selected.append(candidate)

    return sorted(
        selected,
        key=lambda item: item["start"],
    )


def protect_terms(text, entities):

    spans = protection_spans(text, entities)

    output = text

    replacements = {}

    for index, span in reversed(
        list(enumerate(spans))
    ):

        placeholder = f"ZXQ{index}QXZ"

        replacements[placeholder] = (
            span["canonical"]
        )

        output = (
            output[:span["start"]]
            + placeholder
            + output[span["end"]:]
        )

    return output, replacements


def expand_terms(text, entities):

    output = text

    for span in reversed(
        protection_spans(text, entities)
    ):

        output = (
            output[:span["start"]]
            + span["canonical"]
            + output[span["end"]:]
        )

    return output


def restore_terms(text, replacements):

    output = text
    missing = []

    for placeholder, replacement in replacements.items():

        pattern = r"\s*".join(
            re.escape(char)
            for char in placeholder
        )

        output, count = re.subn(
            pattern,
            lambda _: replacement,
            output,
            flags=re.IGNORECASE,
        )

        if count != 1:
            missing.append(replacement)

    return normalize_space(output), missing


# ============================================================
# TRANSLATION VALIDATION
# ============================================================

def extract_numbers(text, language="cs"):

    text = (text or "").replace("\u00a0", " ")

    text = re.sub(
        r"(?<=\d) (?=\d{3}(?:\D|$))",
        "",
        text,
    )

    values = re.findall(
        r"(?<!\w)\d+(?:[.,]\d+)*(?!\w)",
        text,
    )

    numbers = []

    for value in values:

        if language == "cs":

            if "," in value:
                value = value.replace(".", "")
                value = value.replace(",", ".")

        else:

            if "," in value and "." in value:

                if value.rfind(",") > value.rfind("."):
                    value = value.replace(".", "")
                    value = value.replace(",", ".")

                else:
                    value = value.replace(",", "")

            elif "," in value:

                if len(value.split(",")[-1]) == 3:
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


def validate_translation(
    original,
    translated,
    entities=None,
    truncated=False,
):

    entities = entities or []

    warnings = []

    original = normalize_space(original)
    translated = normalize_space(translated)

    if not original:
        return []

    if not translated:
        return ["empty_translation"]

    if truncated:
        warnings.append("generation_truncated")

    original_words = len(original.split())
    english_words = len(translated.split())

    if original_words >= 8:

        if english_words < original_words * 0.45:

            warnings.append(
                "suspiciously_short"
            )

    original_numbers = extract_numbers(
        original, "cs"
    )

    english_numbers = extract_numbers(
        translated, "en"
    )

    missing_numbers = (
        original_numbers - english_numbers
    )

    if missing_numbers:

        warnings.append(
            "missing_numbers:"
            + ",".join(
                sorted(missing_numbers)
            )
        )

    unexpected_numbers = (
        english_numbers - original_numbers
    )

    if original_numbers and unexpected_numbers:

        warnings.append(
            "unexpected_numbers:"
            + ",".join(
                sorted(unexpected_numbers)
            )
        )

    if "%" in original or regex_match(
        r"\bprocent\w*",
        original,
    ):

        if not (
            "%" in translated
            or regex_match(
                r"\bpercent\b|\bpercentage\b",
                translated,
            )
        ):

            warnings.append(
                "missing_percentage"
            )

    normalized_english = fold_accents(
        translated
    )

    for span in protection_spans(
        original,
        entities,
    ):

        expected = span["canonical"]

        if fold_accents(expected) not in normalized_english:

            warnings.append(
                f"missing_entity_or_term:{expected}"
            )

    for source_pattern, target_pattern, reason in (
        SEMANTIC_CHECKS
    ):

        if (
            regex_match(source_pattern, original)
            and regex_match(target_pattern, translated)
        ):

            warnings.append(reason)

    if regex_match(
        r"Z\s*X\s*Q\s*\d+\s*Q\s*X\s*Z",
        translated,
    ):

        warnings.append(
            "unrestored_placeholder"
        )

    if (
        original_words >= 6
        and original.casefold()
        == translated.casefold()
    ):

        warnings.append(
            "untranslated_text"
        )

    if regex_match(
        r"\bKč\b|\bkorun\w*",
        original,
    ):

        if not regex_match(
            r"\bCZK\b|\bcrowns?\b|\bkorun\w*",
            translated,
        ):

            warnings.append(
                "currency_not_preserved"
            )

    return sorted(set(warnings))


# ============================================================
# NLLB TRANSLATION MODEL
# ============================================================

def load_translator():

    global TOKENIZER
    global MODEL
    global TORCH

    if MODEL is not None:
        return

    import torch

    from transformers import (
        AutoTokenizer,
        AutoModelForSeq2SeqLM,
    )

    TORCH = torch

    started = time.monotonic()

    print(f"[MODEL] Loading {MODEL_NAME}")

    torch.set_num_threads(
        min(
            CPU_THREADS,
            os.cpu_count() or CPU_THREADS,
        )
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


def token_length(text):

    return len(
        TOKENIZER.encode(
            text,
            add_special_tokens=True,
        )
    )


def model_translate(texts, beams=None):

    if not texts:
        return []

    load_translator()

    lengths = [
        token_length(text)
        for text in texts
    ]

    if max(lengths) > MAX_INPUT_TOKENS:

        raise ValueError(
            "Input exceeds 512-token limit"
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
            "Invalid NLLB target token"
        )

    beam_count = beams or NUM_BEAMS

    output_limit = min(
        MAX_NEW_TOKENS,
        max(
            64,
            int(max(lengths) * 2.5) + 24,
        ),
    )

    with TORCH.inference_mode():

        outputs = MODEL.generate(
            **inputs,
            forced_bos_token_id=target_token,
            max_new_tokens=output_limit,
            num_beams=beam_count,
            do_sample=False,
            early_stopping=beam_count > 1,
            use_cache=True,
        )

    translations = TOKENIZER.batch_decode(
        outputs,
        skip_special_tokens=True,
    )

    STATS["model_sentences"] += len(texts)

    results = []

    for sequence, translation in zip(
        outputs,
        translations,
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
            "text": normalize_space(translation),
            "truncated": truncated,
        })

    return results


# ============================================================
# SENTENCE TRANSLATION
# ============================================================

def translate_sentences(
    sentences,
    sentence_cache,
    entity_cache,
):

    sentences = list(
        dict.fromkeys(
            normalize_space(sentence)
            for sentence in sentences
            if normalize_space(sentence)
        )
    )

    entities_by_sentence = detect_entities(
        sentences,
        entity_cache,
    )

    results = {}
    pending = []

    for sentence in sentences:

        entities = entities_by_sentence.get(
            sentence, []
        )

        key = sentence_cache_key(
            sentence,
            entities,
        )

        cached = sentence_cache.get(key)

        if cached and cached.get("translation"):

            results[sentence] = {
                "translation": cached["translation"],
                "warnings": [],
                "entities": entities,
                "cached": True,
            }

            STATS["sentence_cache_hits"] += 1

        else:

            protected, replacements = protect_terms(
                sentence,
                entities,
            )

            pending.append({
                "original": sentence,
                "protected": protected,
                "replacements": replacements,
                "entities": entities,
                "cache_key": key,
            })

            STATS["sentence_cache_misses"] += 1

    if not pending:
        return results

    load_translator()

    pending.sort(
        key=lambda row: token_length(
            row["protected"]
        )
    )

    # Initial translation

    for start in range(
        0, len(pending), BATCH_SIZE
    ):

        batch = pending[
            start:start + BATCH_SIZE
        ]

        started = time.monotonic()

        valid_batch = []

        for row in batch:

            if token_length(
                row["protected"]
            ) > MAX_INPUT_TOKENS:

                results[row["original"]] = {
                    "translation": "",
                    "warnings": ["input_too_long"],
                    "entities": row["entities"],
                    "cached": False,
                }

            else:
                valid_batch.append(row)

        if not valid_batch:
            continue

        outputs = model_translate([
            row["protected"]
            for row in valid_batch
        ])

        for row, result in zip(
            valid_batch,
            outputs,
        ):

            translated, missing = restore_terms(
                result["text"],
                row["replacements"],
            )

            warnings = validate_translation(
                row["original"],
                translated,
                row["entities"],
                result["truncated"],
            )

            if missing:

                warnings.append(
                    "missing_protected_placeholders"
                )

            results[row["original"]] = {
                "translation": translated,
                "warnings": sorted(set(warnings)),
                "entities": row["entities"],
                "cached": False,
            }

        print(
            f"[SENTENCE BATCH] "
            f"{len(valid_batch)} sentences | "
            f"{time.monotonic() - started:.1f}s"
        )

    # Retry suspicious translations with direct expansion
    # instead of artificial placeholders.

    retry_rows = [

        row for row in pending

        if results[row["original"]]["warnings"]
        and "input_too_long" not in
        results[row["original"]]["warnings"]

    ]

    if retry_rows:

        print(
            f"[VALIDATION] Retrying "
            f"{len(retry_rows)} sentences"
        )

    for start in range(
        0, len(retry_rows), BATCH_SIZE
    ):

        batch = retry_rows[
            start:start + BATCH_SIZE
        ]

        expanded = [
            expand_terms(
                row["original"],
                row["entities"],
            )
            for row in batch
        ]

        if any(
            token_length(text) > MAX_INPUT_TOKENS
            for text in expanded
        ):
            continue

        outputs = model_translate(
            expanded,
            beams=max(5, NUM_BEAMS),
        )

        for row, output in zip(batch, outputs):

            original = row["original"]

            warnings = validate_translation(
                original,
                output["text"],
                row["entities"],
                output["truncated"],
            )

            previous = results[original]

            if len(warnings) < len(
                previous["warnings"]
            ):

                results[original] = {
                    "translation": output["text"],
                    "warnings": warnings,
                    "entities": row["entities"],
                    "cached": False,
                }

    # Cache only validated outputs.

    for row in pending:

        original = row["original"]
        result = results[original]

        if not result["warnings"]:

            sentence_cache[
                row["cache_key"]
            ] = {
                "original": original,
                "translation": result["translation"],
                "translation_version": TRANSLATION_VERSION,
                "validation_version": VALIDATION_VERSION,
                "ner_version": NER_VERSION,
            }

    return results


# ============================================================
# ARTICLE ARCHIVE
# ============================================================

def load_archive():

    if not ARCHIVE_JSON.exists():
        return {}

    data = load_json(ARCHIVE_JSON)

    rows = (
        data if isinstance(data, list)
        else data.get("articles")
    )

    if not isinstance(rows, list):

        raise ValueError(
            "Archive exists but has an invalid structure. "
            "Refusing to overwrite it."
        )

    archive = {}

    for article in rows:

        key = article_fingerprint(article)

        article["fingerprint"] = key

        article.setdefault(
            "source_language",
            SOURCES.get(
                article.get("source", ""),
                {},
            ).get("language", "cs"),
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
            1 if article.get("title_en") else 0,
        )

        article.setdefault("classification_version", 0)
        article.setdefault("validation_version", 0)
        article.setdefault("ner_version", 0)

        article.setdefault("translation_attempts", 0)
        article.setdefault("version_attempts", 0)
        article.setdefault("attempt_version", 0)

        article.setdefault("translation_warnings", [])

        archive[key] = article

    return archive


def sorted_articles(archive):

    return sorted(
        archive.values(),
        key=lambda article: article.get(
            "published_utc", ""
        ),
        reverse=True,
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
            now,
        ),
    )


def prepare_article(row):

    article = {
        **row,

        "id": hash_key(
            row["source"],
            row["url"],
            row["published_utc"],
            normalize_title(row["title_raw"]),
        ),

        "fingerprint": article_fingerprint(row),

        "title_en": None,
        "lead_en": None,

        "translation_status": "pending",

        "translation_version": 0,
        "classification_version": 0,
        "validation_version": 0,
        "ner_version": 0,

        "translation_attempts": 0,
        "version_attempts": 0,
        "attempt_version": 0,

        "translation_provider": None,
        "translation_method": None,

        "translation_error": None,
        "translation_warnings": [],

        "named_entities": [],

        "collected_at_utc": utc_string(utcnow()),
    }

    classify_topics(article)

    if row["source_language"] == "en":

        article["title_en"] = row["title_raw"]
        article["lead_en"] = row["lead_raw"]

        article["translation_status"] = (
            "not_required"
        )

        article["translation_provider"] = (
            "original_english"
        )

    elif (
        not article["relevant"]
        and not TRANSLATE_ALL
    ):

        article["translation_status"] = (
            "not_relevant"
        )

    return article


# ============================================================
# VERSION-AWARE MIGRATION
# ============================================================

def migrate_classifications(archive):

    changed = 0

    for article in archive.values():

        if article.get(
            "classification_version", 0
        ) >= CLASSIFICATION_VERSION:

            continue

        classify_topics(article)

        changed += 1

        status = article.get(
            "translation_status"
        )

        relevant = (
            article["relevant"]
            or TRANSLATE_ALL
        )

        if article["source_language"] == "en":
            continue

        if status == "not_relevant" and relevant:

            if article.get("title_en"):

                article["translation_status"] = (
                    "completed"
                )

            else:

                article["translation_status"] = (
                    "pending"
                )

        elif (
            status in ("pending", "failed")
            and not relevant
        ):

            article["translation_status"] = (
                "not_relevant"
            )

    print(
        f"[MIGRATION] Reclassified {changed} articles"
    )


def current_version_attempts(article):

    if article.get(
        "attempt_version", 0
    ) != TRANSLATION_VERSION:

        return 0

    return article.get(
        "version_attempts", 0
    )


def can_attempt(article):

    return (
        current_version_attempts(article)
        < MAX_TRANSLATION_ATTEMPTS
    )


def needs_upgrade(article, now):

    if article.get("source_language") != "cs":
        return False

    if not (
        article.get("relevant")
        or TRANSLATE_ALL
    ):
        return False

    if not can_attempt(article):
        return False

    if article.get(
        "translation_status"
    ) != "completed":
        return False

    if article.get(
        "translation_version", 0
    ) >= TRANSLATION_VERSION:

        return False

    if within_window(article, now):

        return UPGRADE_ACTIVE_ARTICLES

    return UPGRADE_HISTORICAL_ARTICLES


# ============================================================
# ARTICLE TRANSLATION
# ============================================================

def record_attempt(article):

    article["translation_attempts"] = (
        article.get("translation_attempts", 0) + 1
    )

    if article.get(
        "attempt_version"
    ) != TRANSLATION_VERSION:

        article["version_attempts"] = 0

    article["attempt_version"] = (
        TRANSLATION_VERSION
    )

    article["version_attempts"] += 1


def translate_article_group(
    articles,
    sentence_cache,
    entity_cache,
    upgrade=False,
):

    sentence_map = {}
    all_sentences = []

    for article in articles:

        title_sentences = split_sentences(
            article.get("title_raw")
        )

        lead_sentences = split_sentences(
            article.get("lead_raw")
        )

        sentence_map[
            article["fingerprint"]
        ] = {
            "title": title_sentences,
            "lead": lead_sentences,
        }

        all_sentences.extend(title_sentences)
        all_sentences.extend(lead_sentences)

    translations = translate_sentences(
        all_sentences,
        sentence_cache,
        entity_cache,
    )

    completed = 0
    flagged = 0

    for article in articles:

        record_attempt(article)

        mapping = sentence_map[
            article["fingerprint"]
        ]

        warnings = []
        article_entities = []

        translated_fields = {}

        for field, sentences in mapping.items():

            parts = []

            for index, sentence in enumerate(
                sentences
            ):

                result = translations[sentence]

                parts.append(
                    result["translation"]
                )

                for warning in result["warnings"]:

                    warnings.append(
                        f"{field}_{index + 1}:{warning}"
                    )

                for entity in result["entities"]:

                    article_entities.append({
                        **entity,
                        "field": field,
                        "sentence": index + 1,
                    })

            translated_fields[field] = (
                join_sentences(parts)
            )

        if (
            article.get("title_en")
            and article.get(
                "translation_version", 0
            ) < TRANSLATION_VERSION
        ):

            article.setdefault(
                "previous_translation",
                {
                    "title_en": article.get("title_en"),
                    "lead_en": article.get("lead_en"),
                    "translation_version": article.get(
                        "translation_version", 0
                    ),
                },
            )

        article["title_en"] = translated_fields["title"]
        article["lead_en"] = translated_fields["lead"]

        article["translation_version"] = (
            TRANSLATION_VERSION
        )

        article["validation_version"] = (
            VALIDATION_VERSION
        )

        article["ner_version"] = (
            NER_VERSION
            if ENABLE_NER and not NER_UNAVAILABLE
            else 0
        )

        article["translation_provider"] = MODEL_NAME

        article["translation_method"] = (
            "sentence_by_sentence_with_entity_protection"
        )

        article["translated_at_utc"] = (
            utc_string(utcnow())
        )

        article["translation_error"] = None

        article["translation_warnings"] = (
            sorted(set(warnings))
        )

        article["named_entities"] = (
            article_entities
        )

        article["sentence_counts"] = {
            field: len(sentences)
            for field, sentences in mapping.items()
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

        print(
            f"[{'UPGRADED' if upgrade else 'TRANSLATED'}] "
            f"{article['title_raw']}"
        )

        print(
            f"    -> {article['title_en']}"
        )

        print(
            f"[TOPICS] {article.get('topics', [])}"
        )

        print(
            "[MATCHES] "
            + json.dumps(
                {
                    topic: matches
                    for topic, matches in article.get(
                        "matched_keywords", {}
                    ).items()
                    if matches
                },
                ensure_ascii=False,
            )
        )

        if article_entities:

            print(
                "[ENTITIES] "
                + json.dumps(
                    sorted({
                        entity["canonical"]
                        for entity in article_entities
                    }),
                    ensure_ascii=False,
                )
            )

        if warnings:

            print(
                f"[REVIEW] {warnings}"
            )

    return completed, flagged


# ============================================================
# QUEUE PROCESSING
# ============================================================

def process_queue(
    queue,
    archive,
    sentence_cache,
    entity_cache,
    queue_name,
):

    completed = 0
    flagged = 0
    processed = 0

    for start in range(
        0, len(queue), ARTICLE_GROUP_SIZE
    ):

        if runtime_exceeded():

            print(
                "[INFO] Runtime budget reached. "
                "Remaining articles deferred."
            )

            break

        group = queue[
            start:start + ARTICLE_GROUP_SIZE
        ]

        started = time.monotonic()

        print(
            f"[GROUP] {queue_name}: "
            f"{len(group)} articles"
        )

        try:

            good, review = translate_article_group(
                group,
                sentence_cache,
                entity_cache,
                upgrade=queue_name == "upgrade",
            )

            completed += good
            flagged += review
            processed += len(group)

        except Exception as exc:

            print(
                f"[ERROR] Translation group failed: "
                f"{type(exc).__name__}: {exc}"
            )

            for article in group:

                record_attempt(article)

                article["translation_error"] = str(
                    exc
                )

                if article.get(
                    "translation_status"
                ) != "completed":

                    article["translation_status"] = (
                        "failed"
                    )

            save_archive(
                archive,
                utcnow(),
            )

            save_caches(
                sentence_cache,
                entity_cache,
            )

            break

        save_archive(
            archive,
            utcnow(),
        )

        save_caches(
            sentence_cache,
            entity_cache,
        )

        print(
            f"[GROUP] Completed in "
            f"{time.monotonic() - started:.1f}s"
        )

        print(
            f"[PROGRESS] {queue_name}: "
            f"{processed}/{len(queue)}"
        )

    return completed, flagged


# ============================================================
# OUTPUT FILES
# ============================================================

def is_publishable(article):

    return article.get(
        "translation_status"
    ) in (
        "completed",
        "not_required",
    )


def write_output_files(archive, now):

    all_articles = sorted_articles(archive)

    save_archive(archive, now)

    latest = [

        article for article in all_articles

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
            keep_hours=KEEP_HOURS,
        ),
    )

    for topic in TOPIC_RULES:

        subset = [

            article for article in latest

            if topic in article.get("topics", [])

        ]

        atomic_write_json(
            DATA_DIR / f"articles_{topic}.json",
            build_payload(
                subset,
                now,
                topic=topic,
            ),
        )

        print(
            f"[OK] {topic}: {len(subset)} articles"
        )

    review_articles = [

        article for article in all_articles

        if within_window(article, now)

        and article.get("translation_status")
        == "review_required"

    ]

    atomic_write_json(
        DATA_DIR / "articles_review_required.json",
        build_payload(
            review_articles,
            now,
        ),
    )

    print(
        f"[OK] Latest articles: {len(latest)}"
    )

    print(
        f"[OK] Archive articles: {len(all_articles)}"
    )

    print(
        f"[INFO] Active review-required articles: "
        f"{len(review_articles)}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    now = utcnow()

    print("=" * 70)

    print("[INFO] Czech Media Tracker — Version 3")

    print(f"[INFO] Run time: {utc_string(now)}")
    print(f"[INFO] Translation model: {MODEL_NAME}")
    print(f"[INFO] NER model: {NER_MODEL_NAME}")

    print(
        f"[INFO] Versions: "
        f"translation={TRANSLATION_VERSION}, "
        f"classification={CLASSIFICATION_VERSION}, "
        f"validation={VALIDATION_VERSION}, "
        f"NER={NER_VERSION}"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Load existing data.
    # --------------------------------------------------------

    archive = load_archive()

    sentence_cache = load_cache(
        SENTENCE_CACHE_JSON
    )

    entity_cache = load_cache(
        NER_CACHE_JSON
    )

    print(
        f"[INFO] Existing archive: "
        f"{len(archive)} articles"
    )

    print(
        f"[INFO] Existing cached translations: "
        f"{len(sentence_cache)}"
    )

    print(
        f"[INFO] Existing cached entity results: "
        f"{len(entity_cache)}"
    )

    # --------------------------------------------------------
    # Collect RSS feeds.
    # --------------------------------------------------------

    for source_name, config in SOURCES.items():

        print(
            f"[RSS] Fetching {source_name}"
        )

        try:

            rows = collect_from_rss(
                source_name,
                config,
                now,
            )

        except Exception as exc:

            print(
                f"[WARN] Source failed: "
                f"{source_name}: {exc}"
            )

            continue

        print(
            f"[RSS] {source_name}: "
            f"{len(rows)} recent articles"
        )

        for row in rows:

            key = article_fingerprint(row)

            if key in archive:

                STATS["duplicates_skipped"] += 1

                continue

            article = prepare_article(row)

            archive[key] = article

            STATS["new_articles"] += 1

    # --------------------------------------------------------
    # Reclassify only older classification versions.
    # --------------------------------------------------------

    migrate_classifications(archive)

    write_output_files(archive, now)

    # --------------------------------------------------------
    # Construct translation queues.
    # --------------------------------------------------------

    active = [

        article for article in archive.values()

        if within_window(article, now)

        and article.get("source_language") == "cs"

        and (
            article.get("relevant")
            or TRANSLATE_ALL
        )

    ]

    active.sort(
        key=lambda article: article.get(
            "published_utc", ""
        ),
        reverse=True,
    )

    new_queue = [

        article for article in active

        if article.get("translation_status")
        == "pending"

        and can_attempt(article)

    ][:TRANSLATE_LIMIT]

    retry_queue = [

        article for article in active

        if article.get("translation_status")
        in ("failed", "review_required")

        and can_attempt(article)

    ][:RETRY_LIMIT]

    upgrade_queue = [

        article for article in sorted_articles(archive)

        if needs_upgrade(article, now)

    ][:UPGRADE_LIMIT]

    print(
        f"[QUEUE] New translations: {len(new_queue)}"
    )

    print(
        f"[QUEUE] Retries: {len(retry_queue)}"
    )

    print(
        f"[QUEUE] One-time upgrades: "
        f"{len(upgrade_queue)}"
    )

    # --------------------------------------------------------
    # Process new articles, upgrades and bounded retries.
    # --------------------------------------------------------

    for queue_name, queue in (
        ("new", new_queue),
        ("upgrade", upgrade_queue),
        ("retry", retry_queue),
    ):

        if not queue or runtime_exceeded():
            continue

        completed, flagged = process_queue(
            queue,
            archive,
            sentence_cache,
            entity_cache,
            queue_name,
        )

        STATS[
            f"{queue_name}_completed"
        ] += completed

        STATS[
            f"{queue_name}_flagged"
        ] += flagged

    # --------------------------------------------------------
    # Save final archive and outputs.
    # --------------------------------------------------------

    save_caches(
        sentence_cache,
        entity_cache,
    )

    write_output_files(
        archive,
        utcnow(),
    )

    statuses = Counter(

        article.get(
            "translation_status",
            "pending",
        )

        for article in archive.values()

    )

    print("=" * 70)

    print(
        "[OK] Czech Media Tracker Version 3 completed"
    )

    for key, value in sorted(STATS.items()):

        print(
            f"[STATS] {key}: {value}"
        )

    for status, count in sorted(statuses.items()):

        print(
            f"[STATUS] {status}: {count}"
        )

    print(
        f"[CACHE] Sentence entries: "
        f"{len(sentence_cache)}"
    )

    print(
        f"[CACHE] Entity entries: "
        f"{len(entity_cache)}"
    )

    print(
        f"[INFO] Runtime: "
        f"{time.monotonic() - RUN_START:.1f}s"
    )

    print("=" * 70)

    return 0


if __name__ == "__main__":

    raise SystemExit(main())
