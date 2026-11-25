import os
import re
import json
import time
import uuid
import html
import hashlib
import calendar
import unicodedata

from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests
import feedparser


# ============================================================
# CONFIGURATION
# ============================================================

COUNTRY = "Czech Republic"
COUNTRY_CODE = "CZ"

KEEP_HOURS = 48
ARCHIVE_RETENTION_DAYS = 90

TRANSLATION_VERSION = 4
CLASSIFICATION_VERSION = 4
VALIDATION_VERSION = 4

MONTHLY_CHARACTER_LIMIT = int(
    os.getenv("MONTHLY_CHARACTER_LIMIT", "1800000")
)

TRANSLATION_BATCH_SIZE = int(
    os.getenv("TRANSLATION_BATCH_SIZE", "25")
)

MAX_TRANSLATION_ATTEMPTS = 3

REQUEST_TIMEOUT = 60
RSS_TIMEOUT = 30

MAX_AZURE_REQUEST_CHARACTERS = 40000

AZURE_ENDPOINT = (
    "https://api.cognitive.microsofttranslator.com/translate"
)

AZURE_KEY = os.getenv("AZURE_TRANSLATOR_KEY", "")
AZURE_REGION = os.getenv("AZURE_TRANSLATOR_REGION", "eastus2")

DATA_DIR = Path("data")

ARCHIVE_PATH = DATA_DIR / "articles_archive.json"
CACHE_PATH = DATA_DIR / "translation_cache.json"
USAGE_PATH = DATA_DIR / "api_usage.json"

USER_AGENT = (
    "CzechiaLocalNewsCollector/4.0"
)


# ============================================================
# RSS SOURCES
# ============================================================

SOURCES = [
    {
        "name": "iROZHLAS - Domestic",
        "url": "https://www.irozhlas.cz/rss/irozhlas/section/zpravy-domov",
        "language": "cs",
    },
    {
        "name": "iROZHLAS - Economy",
        "url": "https://www.irozhlas.cz/rss/irozhlas/section/ekonomika",
        "language": "cs",
    },
    {
        "name": "České noviny / ČTK - Czech Republic",
        "url": "https://www.ceskenoviny.cz/sluzby/rss/cr.php",
        "language": "cs",
    },
    {
        "name": "České noviny / ČTK - Economy",
        "url": "https://www.ceskenoviny.cz/sluzby/rss/ekonomika.php",
        "language": "cs",
    },
    {
        "name": "ČT24",
        "url": "https://ct24.ceskatelevize.cz/rss",
        "language": "cs",
    },
    {
        "name": "Novinky",
        "url": "https://www.novinky.cz/rss",
        "language": "cs",
    },
    {
        "name": "Seznam Zprávy",
        "url": "https://www.seznamzpravy.cz/rss",
        "language": "cs",
    },
    {
        "name": "iDNES.cz",
        "url": "https://servis.idnes.cz/rss.aspx?c=zpravodaj",
        "language": "cs",
    },
    {
        "name": "Radio Prague International (EN)",
        "url": "https://english.radio.cz/rcz-rss/en",
        "language": "en",
    },
]


# ============================================================
# TOPIC CLASSIFICATION
# ============================================================

# Each rule has:
#   concept name
#   strength: 3 = strong, 2 = moderate, 1 = weak
#   Czech regular expression
#   English regular expression
#
# Headline matches receive 3x weight.
# Lead matches receive 2x weight.


TOPIC_RULES = {

    "diplomacy": [

        ("summit", 3,
         r"\bsummit\w*",
         r"\bsummits?\b"),

        ("ambassador", 3,
         r"\bvelvyslan\w*|\bambasád\w*",
         r"\bambassadors?\b|\bembass(?:y|ies)\b"),

        ("diplomacy", 3,
         r"\bdiplomac\w*|\bdiplomat\w*",
         r"\bdiplomac\w*"),

        ("peace_negotiations", 3,
         r"\bmírov\w*\s+jednání|\bjednání\s+o\s+ukončení\s+války",
         r"\bpeace talks\b|\bpeace negotiations\b"),

        ("ceasefire", 3,
         r"\bpříměří\b",
         r"\bceasefire\b"),

        ("bilateral_relations", 3,
         r"\bbilateráln\w*|\bmezistátn\w*",
         r"\bbilateral\b"),

        ("foreign_ministry", 3,
         r"\bministr\w*\s+zahraničí|\bministerstv\w*\s+zahraničí",
         r"\bforeign minister\b|\bforeign ministry\b"),

        ("state_visit", 3,
         r"\bstátn\w*\s+návštěv\w*|\boficiáln\w*\s+návštěv\w*",
         r"\bstate visit\b|\bofficial visit\b"),

        ("negotiations", 2,
         r"\bvyjednáv\w*|\bjednání\b",
         r"\bnegotiations?\b|\btalks\b"),

        ("international_cooperation", 3,
         r"\bmezinárodn\w*\s+spoluprác\w*",
         r"\binternational cooperation\b"),

    ],

    "military": [

        ("war", 3,
         r"\bválk\w*|\bválce\b|\bváleč\w*",
         r"\bwars?\b|\bwarfare\b"),

        ("armed_forces", 3,
         r"\barmád\w*|\bozbrojen\w*\s+sil\w*",
         r"\barmy\b|\barmed forces\b|\bmilitary\b"),

        ("nato", 3,
         r"\b(?-i:NATO)\b",
         r"\b(?-i:NATO)\b"),

        ("defense", 3,
         r"\bobran\w*|\bobranář\w*",
         r"\bdefen[cs]e\b"),

        ("missiles", 3,
         r"\braket\w*|\bstřel\w*\s+s\s+plochou",
         r"\bmissiles?\b"),

        ("air_defense", 3,
         r"\bprotivzdušn\w*|\bsestřelov\w*|\bsestřelil\w*",
         r"\bair defen[cs]e\b|\bmissile interception\b"),

        ("drones", 3,
         r"\bdron\w*|\bbezpilotn\w*",
         r"\bdrones?\b|\bunmanned aerial\b"),

        ("military_equipment", 3,
         r"\bmunic\w*|\bzbroj\w*|\bdělostřel\w*|\btank\w*",
         r"\bammunition\b|\bweapons\b|\bartillery\b|\btanks?\b"),

        ("hybrid_warfare", 3,
         r"\bhybridn\w*\s+(?:válk|válc|hrozb|hrozeb)\w*",
         r"\bhybrid warfare\b|\bhybrid threats?\b"),

        ("cybersecurity", 3,
         r"\bkybernetick\w*\s+(?:útok|bezpeč|hrozb)\w*",
         r"\bcybersecurity\b|\bcyberattacks?\b"),

        ("intelligence", 3,
         r"\bzpravodajsk\w*\s+služb\w*|\bšpionáž\w*",
         r"\bintelligence services?\b|\bespionage\b"),

        ("mobilization", 3,
         r"\bmobilizac\w*|\bbrann\w*\s+povinnost\w*",
         r"\bmobili[sz]ation\b|\bconscription\b"),

        ("military_aggression", 3,
         r"\brusk\w*\s+agres\w*|\bvojensk\w*\s+agres\w*",
         r"\bmilitary aggression\b|\bRussian aggression\b"),

    ],

    "energy": [

        ("nuclear", 3,
         r"\bjadern\w*|\bjádro\b|\bDukovan\w*|\bTemelín\w*",
         r"\bnuclear\b|\bDukovany\b|\bTemelín\b"),

        ("electricity", 3,
         r"\belektřin\w*|\belektrár\w*|\belektroenerget\w*",
         r"\belectricity\b|\bpower plants?\b"),

        ("natural_gas", 3,
         r"\bzemní\w*\s+plyn\w*|\bplynovod\w*|\bLNG\b",
         r"\bnatural gas\b|\bgas pipelines?\b|\bLNG\b"),

        ("oil", 3,
         r"\brop[ayěou]\b|\bropn\w*|\broppovod\w*|\bropovod\w*",
         r"\bcrude oil\b|\boil pipelines?\b"),

        ("fuel_prices", 3,
         r"\bpohonn\w*\s+hmot\w*|\bbenz[ií]n\w*|\bnaft[ayěou]\b",
         r"\bfuel prices?\b|\bgasoline\b|\bpetrol\b|\bdiesel\b"),

        ("renewables", 3,
         r"\bfotovolta\w*|\bobnoviteln\w*\s+zdroj\w*|\bsolárn\w*",
         r"\bsolar\b|\bphotovoltaic\b|\brenewable energy\b"),

        ("energy_security", 3,
         r"\benergetick\w*\s+bezpečnost\w*",
         r"\benergy security\b"),

        ("heating", 3,
         r"\bteplár\w*|\bteplárenstv\w*",
         r"\bdistrict heating\b|\bheating plants?\b"),

        ("energy_policy", 3,
         r"\benergetik\w*|\benergetick\w*\s+politik\w*",
         r"\benergy policy\b|\benergy sector\b"),

        ("coal", 3,
         r"\buhlí\b|\buheln\w*",
         r"\bcoal\b"),

        ("power_grid", 3,
         r"\bpřenosov\w*\s+soustav\w*|\belektrick\w*\s+sít\w*",
         r"\bpower grid\b|\btransmission grid\b"),

        ("battery_storage", 3,
         r"\bbateriov\w*\s+úložišt\w*",
         r"\bbattery storage\b|\benergy storage\b"),

    ],

    "economy": [

        ("inflation", 3,
         r"\binflac\w*",
         r"\binflation\b"),

        ("interest_rates", 3,
         r"\búrokov\w*\s+sazb\w*",
         r"\binterest rates?\b"),

        ("central_bank", 3,
         r"\bČNB\b|\bcentráln\w*\s+bank\w*",
         r"\bcentral bank\b|\bCzech National Bank\b"),

        ("government_budget", 3,
         r"\bstátn\w*\s+rozpoč\w*",
         r"\bstate budget\b|\bgovernment budget\b"),

        ("taxation", 3,
         r"\bdaň\w*|\bdaní\b|\bdaňov\w*",
         r"\btax(?:es|ation)?\b"),

        ("unemployment", 3,
         r"\bnezaměstnan\w*",
         r"\bunemployment\b"),

        ("economic_growth", 3,
         r"\bHDP\b|\bhrub\w*\s+domác\w*\s+produkt\w*|\bhospodářsk\w*\s+růst\w*",
         r"\bGDP\b|\beconomic growth\b"),

        ("subsidies", 3,
         r"\bdotac\w*|\bdotačn\w*",
         r"\bsubsid(?:y|ies)\b"),

        ("wages", 3,
         r"\bmzd\w*|\bplatov\w*",
         r"\bwages?\b|\bsalar(?:y|ies)\b"),

        ("stock_market", 3,
         r"\bburz\w*|\bakciov\w*|\bkapitálov\w*\s+trh\w*",
         r"\bstock market\b|\bcapital markets?\b"),

        ("commodity_markets", 3,
         r"\bcen\w*\s+zlata\b|\bkomodit\w*|\bcen\w*\s+stříbra\b",
         r"\bgold prices?\b|\bcommodity markets?\b"),

        ("investment", 2,
         r"\binvestic\w*|\binvestor\w*",
         r"\binvestments?\b|\binvestors?\b"),

        ("trade", 3,
         r"\bzahraničn\w*\s+obchod\w*|\bexport\w*|\bimport\w*|\bcelní\w*",
         r"\binternational trade\b|\bexports?\b|\bimports?\b|\btariffs?\b"),

        ("housing_market", 3,
         r"\btrh\w*\s+s\s+byty\b|\bhypoték\w*|\bnájm\w*",
         r"\bhousing market\b|\bmortgages?\b"),

        ("economic_policy", 3,
         r"\bekonomick\w*\s+politik\w*|\bhospodářsk\w*\s+politik\w*",
         r"\beconomic policy\b|\bfiscal policy\b"),

        ("price_caps", 3,
         r"\bcenov\w*\s+strop\w*",
         r"\bprice caps?\b"),

        ("currency", 1,
         r"\bkorun\w*|\bKč\b|\beur\w*|\bdolar\w*",
         r"\bcrowns\b|\bCZK\b|\beuros?\b|\bdollars?\b"),

        ("prices", 1,
         r"\bcen[ayěou]\b|\bzdraž\w*|\bzlevn\w*",
         r"\bprices?\b"),

        ("generic_budget", 1,
         r"\brozpoč\w*",
         r"\bbudgets?\b"),

    ],

    "domestic_politics": [

        ("government", 2,
         r"\bvlád\w*|\bministerstv\w*|\bkabinet\w*",
         r"\bgovernment\b|\bministry\b|\bcabinet\b"),

        ("parliament", 3,
         r"\bsněmovn\w*|\bsenát\w*|\bposlan\w*|\bparlament\w*",
         r"\bparliament\b|\bsenate\b|\bchamber of deputies\b"),

        ("legislation", 3,
         r"\bzákon\w*|\bnovel\w*|\blegislativ\w*",
         r"\blegislation\b|\bbill\b|\blawmaking\b"),

        ("elections", 3,
         r"\bvolb\w*|\bvoleb\w*",
         r"\belections?\b|\belectoral\b"),

        ("coalition", 3,
         r"\bkoalic\w*|\bopozic\w*",
         r"\bcoalition\b|\bopposition\b"),

        ("political_parties", 3,
         r"\b(?-i:ANO|ODS|SPD|STAN|TOP\s*09|KDU-ČSL)\b|\bPiráti\b",
         r"\b(?-i:ANO|ODS|SPD|STAN|TOP\s*09|KDU-ČSL)\b|\bPirate Party\b"),

        ("municipal_government", 3,
         r"\bstarost\w*|\bprimátor\w*|\bzastupitel\w*|\bhejtman\w*",
         r"\bmayors?\b|\bmunicipal council\b|\bregional governors?\b"),

        ("public_broadcasting", 3,
         r"\bČesk\w*\s+televiz\w*|\bČesk\w*\s+rozhlas\w*|\bČT\b|\bČRo\b",
         r"\bCzech Television\b|\bCzech Radio\b|\bpublic broadcasting\b"),

        ("constitutional_affairs", 3,
         r"\bústavn\w*\s+soud\w*|\bústavn\w*\s+zákon\w*",
         r"\bconstitutional court\b|\bconstitutional law\b"),

        ("political_corruption", 3,
         r"\bstřet\w*\s+zájm\w*|\bkorupc\w*",
         r"\bconflicts? of interest\b|\bcorruption\b"),

        ("public_policy", 3,
         r"\bvládn\w*\s+nařízení|\bveřejn\w*\s+politik\w*",
         r"\bpublic policy\b|\bgovernment regulations?\b"),

        ("leadership", 1,
         r"\bpremiér\w*|\bprezident\w*|\bministr\w*",
         r"\bprime minister\b|\bpresident\b|\bminister\b"),

        ("czech_leaders", 3,
         r"\bBabiš\w*|\bOkamur\w*|\bFial\w*|\bPetr\w*\s+Pav\w*",
         r"\bBabiš\b|\bOkamura\b|\bFiala\b|\bPetr Pavel\b"),

    ],

    "foreign_affairs": [

        ("eu_policy", 3,
         r"\bEvropsk\w*\s+uni\w*|\bEvropsk\w*\s+komis\w*|\beuroparlament\w*|\beuroposlan\w*|\bEU\b",
         r"\bEuropean Union\b|\bEuropean Commission\b|\bEuropean Parliament\b|\bEU\b"),

        ("international_security", 3,
         r"\bmezinárodn\w*\s+bezpečnost\w*",
         r"\binternational security\b"),

        ("sanctions", 3,
         r"\bsankc\w*",
         r"\bsanctions?\b"),

        ("foreign_policy", 3,
         r"\bzahraničn\w*\s+politik\w*",
         r"\bforeign policy\b"),

        ("international_organizations", 3,
         r"\bOSN\b|\bSvětov\w*\s+bank\w*|\bMezinárodn\w*\s+měnov\w*",
         r"\bUnited Nations\b|\bWorld Bank\b|\bIMF\b"),

        ("ukraine_conflict", 3,
         r"\bválk\w*\s+na\s+Ukrajin\w*|\brusk\w*\s+agres\w*",
         r"\bwar in Ukraine\b|\bRussian aggression\b"),

        ("humanitarian", 3,
         r"\bhumanitárn\w*|\buprchlík\w*",
         r"\bhumanitarian\b|\brefugees?\b"),

        ("foreign_government", 2,
         r"\bukrajinsk\w*\s+prezident\w*|\brusk\w*\s+prezident\w*|\bamerick\w*\s+prezident\w*",
         r"\bUkrainian president\b|\bRussian president\b|\bUS president\b"),

        ("international_negotiations", 3,
         r"\bmírov\w*\s+jednání|\bjednání\s+o\s+ukončení\s+války",
         r"\bpeace negotiations\b|\bpeace talks\b"),

        ("migration", 3,
         r"\bmigrační\w*\s+politik\w*|\bnelegáln\w*\s+migrac\w*",
         r"\bmigration policy\b|\billegal migration\b"),

    ],

    "local_events": [

        ("demonstrations", 3,
         r"\bdemonstrac\w*|\bprotest\w*",
         r"\bdemonstrations?\b|\bprotests?\b"),

        ("infrastructure", 3,
         r"\bstavb\w*\s+most\w*|\bstavební\w*\s+povolení|\bnov\w*\s+lávk\w*",
         r"\bbridge construction\b|\bconstruction permit\b|\binfrastructure project\b"),

        ("public_transport", 3,
         r"\bveřejn\w*\s+doprav\w*|\btramvaj\w*|\bželezničn\w*\s+trat\w*",
         r"\bpublic transport\b|\btramways?\b|\brailway line\b"),

        ("natural_disaster", 3,
         r"\bpovodn\w*|\bzáplav\w*|\bzemětřesen\w*|\bsesuv\w*\s+půd\w*",
         r"\bfloods?\b|\bearthquakes?\b|\blandslides?\b"),

        ("public_emergency", 3,
         r"\bnouzov\w*\s+stav\w*|\bevakuac\w*",
         r"\bstate of emergency\b|\bevacuations?\b"),

        ("local_services", 3,
         r"\bměstsk\w*\s+nemocnic\w*|\bměstsk\w*\s+služb\w*",
         r"\bmunicipal hospital\b|\bmunicipal services\b"),

        ("public_events", 2,
         r"\bveřejn\w*\s+shromážděn\w*|\bslavnost\w*",
         r"\bpublic gathering\b|\bpublic events?\b"),

    ],

}


TOPICS = list(TOPIC_RULES.keys())

TOPIC_THRESHOLDS = {
    topic: 6
    for topic in TOPICS
}


CZECH_CONTEXT = re.compile(
    r"\bČesk\w*|\bČesko\b|\bČR\b|\bPra[hz]\w*|"
    r"\bBrn\w*|\bOstrav\w*|\bBabiš\w*|"
    r"\b(?-i:ANO|ODS|SPD|STAN|TOP\s*09|KDU-ČSL)\b|"
    r"\bCzech\b|\bPrague\b",
    re.IGNORECASE
)


# ============================================================
# TEXT AND DATE UTILITIES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_string(value=None):
    value = value or utc_now()

    return value.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def parse_date(value):

    if not value:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    try:
        return datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        ).astimezone(timezone.utc)

    except (ValueError, TypeError):
        pass

    try:
        result = parsedate_to_datetime(str(value))

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        return result.astimezone(timezone.utc)

    except Exception:
        return None


def entry_date(entry):

    for field in ("published_parsed", "updated_parsed"):

        value = entry.get(field)

        if value:

            return datetime.fromtimestamp(
                calendar.timegm(value),
                tz=timezone.utc
            )

    for field in ("published", "updated", "created"):

        value = parse_date(entry.get(field))

        if value:
            return value

    return None


def clean_text(value):

    if not value:
        return ""

    value = html.unescape(str(value))

    value = re.sub(
        r"<[^>]+>",
        " ",
        value
    )

    value = unicodedata.normalize("NFC", value)

    value = re.sub(r"\s+", " ", value)

    return value.strip()


def normalize_url(url):

    parts = urlsplit(url)

    return urlunsplit((
        parts.scheme.lower(),
        parts.netloc.lower(),
        parts.path.rstrip("/"),
        "",
        ""
    ))


def sha1(value):

    return hashlib.sha1(
        value.encode("utf-8")
    ).hexdigest()


def atomic_json_write(path, value):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    with open(
        temp_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            value,
            file,
            ensure_ascii=False,
            indent=2
        )

    os.replace(temp_path, path)


def read_json(path, default):

    if not path.exists():
        return default

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


# ============================================================
# CLASSIFIER
# ============================================================

def classify_topics(article):

    title = article.get("title_raw", "")
    lead = article.get("lead_raw", "")

    language = article.get("source_language", "cs")

    combined = title + " " + lead

    czech_context = bool(
        CZECH_CONTEXT.search(combined)
        or "Domestic" in article.get("source", "")
        or "/domaci" in article.get("url", "")
        or "/zpravy-domov" in article.get("url", "")
    )

    scores = {}
    matched = {}
    details = {}
    topics = []

    for topic, rules in TOPIC_RULES.items():

        score = 0
        strong_match = False
        matches = []
        concept_details = []

        for concept, strength, cs_pattern, en_pattern in rules:

            pattern = (
                en_pattern
                if language == "en"
                else cs_pattern
            )

            headline_match = bool(
                re.search(
                    pattern,
                    title,
                    re.IGNORECASE
                )
            )

            lead_match = bool(
                re.search(
                    pattern,
                    lead,
                    re.IGNORECASE
                )
            )

            if not headline_match and not lead_match:
                continue

            weight = (
                3 if headline_match else 2
            )

            points = strength * weight

            score += points

            if strength == 3:
                strong_match = True

            matches.append(concept)

            concept_details.append({
                "concept": concept,
                "strength": strength,
                "headline": headline_match,
                "lead": lead_match,
                "points": points,
            })

        scores[topic] = score
        matched[topic] = matches
        details[topic] = concept_details

        qualifies = (
            score >= TOPIC_THRESHOLDS[topic]
            and strong_match
        )

        if topic in (
            "domestic_politics",
            "local_events"
        ) and not czech_context:

            qualifies = False

        if qualifies:
            topics.append(topic)

    article["topic_scores"] = scores
    article["matched_keywords"] = matched
    article["classification_details"] = details

    article["topics"] = topics
    article["relevant"] = bool(topics)

    article["primary_topic"] = (
        max(topics, key=lambda topic: scores[topic])
        if topics
        else None
    )

    article["classification_version"] = (
        CLASSIFICATION_VERSION
    )

    return article


# ============================================================
# ARCHIVE
# ============================================================

def load_archive():

    payload = read_json(
        ARCHIVE_PATH,
        {"articles": []}
    )

    archive = {}

    for article in payload.get("articles", []):

        url = normalize_url(
            article.get("url", "")
        )

        if url:

            article["url"] = url
            archive[url] = article

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


def save_archive(archive):

    articles = sorted_articles(archive)

    atomic_json_write(
        ARCHIVE_PATH,
        {
            "country": COUNTRY,
            "country_code": COUNTRY_CODE,
            "updated_at_utc": utc_string(),
            "count": len(articles),
            "articles": articles,
        }
    )


def active_articles(archive):

    cutoff = utc_now() - timedelta(
        hours=KEEP_HOURS
    )

    results = []

    for article in sorted_articles(archive):

        published = parse_date(
            article.get("published_utc")
        )

        if published and published >= cutoff:
            results.append(article)

    return results


def prune_archive(archive):

    cutoff = utc_now() - timedelta(
        days=ARCHIVE_RETENTION_DAYS
    )

    remove = []

    for url, article in archive.items():

        published = parse_date(
            article.get("published_utc")
        )

        if published and published < cutoff:
            remove.append(url)

    for url in remove:
        del archive[url]

    return len(remove)


# ============================================================
# RSS COLLECTION
# ============================================================

def fetch_source(source):

    response = requests.get(
        source["url"],
        headers={
            "User-Agent": USER_AGENT
        },
        timeout=RSS_TIMEOUT
    )

    response.raise_for_status()

    feed = feedparser.parse(
        response.content
    )

    articles = []

    cutoff = utc_now() - timedelta(
        hours=KEEP_HOURS
    )

    for entry in feed.entries:

        title = clean_text(
            entry.get("title", "")
        )

        lead = clean_text(
            entry.get("summary")
            or entry.get("description")
            or ""
        )

        url = normalize_url(
            entry.get("link", "")
        )

        published = entry_date(entry)

        if not title or not url or not published:
            continue

        if published < cutoff:
            continue

        if published > utc_now() + timedelta(hours=1):
            continue

        article = {
            "id": sha1(url),
            "fingerprint": sha1(
                title + "\n" + lead
            ),

            "source": source["name"],
            "source_language": source["language"],

            "url": url,

            "published_utc": utc_string(published),
            "collected_at_utc": utc_string(),

            "title_raw": title,
            "lead_raw": lead,

            "title_en": None,
            "lead_en": None,

            "translation_status": "pending",

            "translation_version": 0,
            "validation_version": 0,

            "translation_attempts": 0,
            "translation_provider": None,
            "translation_method": None,

            "translation_error": None,
            "translation_warnings": [],

        }

        classify_topics(article)

        if source["language"] == "en":

            article["title_en"] = title
            article["lead_en"] = lead

            article["translation_status"] = (
                "not_required"
            )

            article["translation_provider"] = (
                "original_english"
            )

        elif not article["relevant"]:

            article["translation_status"] = (
                "not_relevant"
            )

        articles.append(article)

    return articles


def collect_articles(archive):

    collected = 0
    updated = 0
    unchanged = 0
    failures = 0

    for source in SOURCES:

        try:

            articles = fetch_source(source)

            print(
                f"[RSS] {source['name']}: "
                f"{len(articles)} active articles"
            )

            for article in articles:

                url = article["url"]

                previous = archive.get(url)

                if previous is None:

                    archive[url] = article
                    collected += 1

                elif (
                    previous.get("fingerprint")
                    != article["fingerprint"]
                ):

                    archive[url] = article
                    updated += 1

                else:
                    unchanged += 1

        except Exception as error:

            failures += 1

            print(
                f"[WARN] RSS failure: "
                f"{source['name']}: {error}"
            )

    print(f"[RSS] New articles: {collected}")
    print(f"[RSS] Updated articles: {updated}")
    print(f"[RSS] Unchanged articles: {unchanged}")
    print(f"[RSS] Failed sources: {failures}")

    return failures


# ============================================================
# API USAGE TRACKING
# ============================================================

def current_month():

    return utc_now().strftime("%Y-%m")


def load_usage():

    usage = read_json(
        USAGE_PATH,
        {
            "provider": "Microsoft Azure Translator",
            "months": {}
        }
    )

    usage.setdefault("months", {})

    usage["months"].setdefault(
        current_month(),
        {
            "characters": 0,
            "requests": 0,
        }
    )

    return usage


def used_characters(usage):

    return usage["months"].get(
        current_month(),
        {}
    ).get(
        "characters",
        0
    )


def reserve_characters(usage, characters):

    month = current_month()

    usage["months"].setdefault(
        month,
        {
            "characters": 0,
            "requests": 0,
        }
    )

    if (
        used_characters(usage) + characters
        > MONTHLY_CHARACTER_LIMIT
    ):

        return False

    usage["months"][month]["characters"] += characters
    usage["months"][month]["requests"] += 1

    usage["updated_at_utc"] = utc_string()
    usage["monthly_safety_limit"] = MONTHLY_CHARACTER_LIMIT

    atomic_json_write(
        USAGE_PATH,
        usage
    )

    return True


# ============================================================
# TRANSLATION CACHE
# ============================================================

def load_cache():

    return read_json(
        CACHE_PATH,
        {
            "provider": "Microsoft Azure Translator",
            "entries": {}
        }
    )


def translation_cache_key(text):

    return sha1(
        f"azure-v{TRANSLATION_VERSION}|cs|en|{text}"
    )


def save_cache(cache):

    atomic_json_write(
        CACHE_PATH,
        cache
    )


# ============================================================
# MICROSOFT TRANSLATOR API
# ============================================================

class TranslationBudgetExceeded(Exception):
    pass


class TranslationUnavailable(Exception):
    pass


def azure_translate(texts, usage):

    if not texts:
        return []

    if not AZURE_KEY:

        raise TranslationUnavailable(
            "AZURE_TRANSLATOR_KEY is missing."
        )

    character_count = sum(
        len(text)
        for text in texts
    )

    if character_count > MAX_AZURE_REQUEST_CHARACTERS:

        raise TranslationUnavailable(
            "Translation request exceeds configured size."
        )

    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_KEY,
        "Ocp-Apim-Subscription-Region": AZURE_REGION,
        "Content-Type": "application/json",
    }

    params = {
        "api-version": "3.0",
        "from": "cs",
        "to": "en",
        "textType": "plain",
    }

    body = [
        {"text": text}
        for text in texts
    ]

    for attempt in range(3):

        if not reserve_characters(
            usage,
            character_count
        ):

            raise TranslationBudgetExceeded(
                "Monthly translation safety limit reached."
            )

        headers["X-ClientTraceId"] = str(
            uuid.uuid4()
        )

        start = time.perf_counter()

        try:

            response = requests.post(
                AZURE_ENDPOINT,
                params=params,
                headers=headers,
                json=body,
                timeout=REQUEST_TIMEOUT
            )

        except requests.RequestException:

            if attempt == 2:

                raise TranslationUnavailable(
                    "Azure network request failed."
                )

            time.sleep(2 ** attempt)
            continue

        elapsed = time.perf_counter() - start

        print(
            f"[AZURE] HTTP {response.status_code} | "
            f"{len(texts)} texts | "
            f"{character_count:,} characters | "
            f"{elapsed:.2f}s"
        )

        if response.status_code == 200:

            payload = response.json()

            if len(payload) != len(texts):

                raise TranslationUnavailable(
                    "Azure returned unexpected result count."
                )

            return [
                result["translations"][0]["text"]
                for result in payload
            ]

        if response.status_code in (401, 403):

            raise TranslationUnavailable(
                f"Azure authentication or quota error: "
                f"HTTP {response.status_code}"
            )

        if response.status_code in (
            429, 500, 502, 503, 504
        ):

            if attempt == 2:

                raise TranslationUnavailable(
                    f"Azure unavailable: "
                    f"HTTP {response.status_code}"
                )

            try:

                delay = float(
                    response.headers.get(
                        "Retry-After",
                        2 ** attempt
                    )
                )

            except ValueError:
                delay = 2 ** attempt

            if response.status_code == 429:
                delay = max(delay, 65)

            print(f"[AZURE] Rate limit or temporary error. Retrying in {delay:.0f}s")

            time.sleep(min(delay, 120))

            continue

            continue

        raise TranslationUnavailable(
            f"Azure API error: "
            f"HTTP {response.status_code}"
        )

    raise TranslationUnavailable(
        "Azure translation failed."
    )


def translate_texts(texts, cache, usage):

    entries = cache.setdefault(
        "entries",
        {}
    )

    results = {}
    missing = {}

    for text in texts:

        if not text:
            results[text] = ""
            continue

        key = translation_cache_key(text)

        if key in entries:

            results[text] = entries[key]["translation"]

        else:

            missing[text] = key

    pending = list(missing)

    batch = []
    batch_characters = 0

    def process_batch(current_batch):

        if not current_batch:
            return

        translations = azure_translate(
            current_batch,
            usage
        )

        for source, target in zip(
            current_batch,
            translations
        ):

            results[source] = target

    for text in pending:

        if (
            batch
            and (
                len(batch) >= 50
                or batch_characters + len(text)
                > MAX_AZURE_REQUEST_CHARACTERS
            )
        ):

            process_batch(batch)

            batch = []
            batch_characters = 0

        batch.append(text)
        batch_characters += len(text)

    process_batch(batch)

    return [
        results[text]
        for text in texts
    ]


# ============================================================
# TRANSLATION VALIDATION
# ============================================================

def extract_numbers(text, language):

    values = []

    pattern = (
        r"(?<!\w)\d+(?:[.,]\d+)*(?!\w)"
    )

    for match in re.findall(pattern, text):

        normalized = match

        if language == "cs":

            normalized = normalized.replace(
                ",",
                "."
            )

        else:

            if (
                "," in normalized
                and "." in normalized
            ):

                normalized = normalized.replace(
                    ",",
                    ""
                )

            elif re.fullmatch(
                r"\d{1,3}(?:,\d{3})+",
                normalized
            ):

                normalized = normalized.replace(
                    ",",
                    ""
                )

        try:

            values.append(
                Decimal(normalized).normalize()
            )

        except InvalidOperation:
            continue

    return Counter(values)


def validate_translation(source, target):

    warnings = []

    if not source:
        return warnings

    if not target:

        return ["empty_translation"]

    if (
        len(source) > 40
        and len(target) < len(source) * 0.35
    ):

        warnings.append(
            "suspiciously_short_translation"
        )

    if (
        len(source) > 40
        and len(target) > len(source) * 3.5
    ):

        warnings.append(
            "suspiciously_long_translation"
        )

    source_numbers = extract_numbers(
        source,
        "cs"
    )

    target_numbers = extract_numbers(
        target,
        "en"
    )

    if source_numbers - target_numbers:

        warnings.append(
            "missing_numeric_values"
        )

    if target_numbers - source_numbers:

        warnings.append(
            "unexpected_numeric_values"
        )

    if "%" in source and "%" not in target:

        if not re.search(
            r"\bpercent\b|\bper cent\b",
            target,
            re.IGNORECASE
        ):

            warnings.append(
                "missing_percentage"
            )

    if re.search(
        r"\bKč\b|\bkorun\w*",
        source,
        re.IGNORECASE
    ):

        if re.search(
            r"\bSEK\b|\bSwedish kronor\b",
            target,
            re.IGNORECASE
        ):

            warnings.append(
                "incorrect_czech_currency"
            )

    if re.search(
        r"\bposlan\w*",
        source,
        re.IGNORECASE
    ) and not re.search(
        r"\beuroposlan\w*|\bEvropsk\w*\s+parlament\w*",
        source,
        re.IGNORECASE
    ):

        if re.search(
            r"\bMEPs?\b",
            target,
            re.IGNORECASE
        ):

            warnings.append(
                "incorrect_parliamentary_terminology"
            )

    if re.search(
        r"\bsestřelov\w*|\bsestřelit\b",
        source,
        re.IGNORECASE
    ):

        if re.search(
            r"\bmissile strikes?\b",
            target,
            re.IGNORECASE
        ):

            warnings.append(
                "possible_missile_interception_error"
            )

    if re.search(
        r"\bpadlých\b|\bpadlí\b",
        source,
        re.IGNORECASE
    ):

        if re.search(
            r"\bcasualties\b",
            target,
            re.IGNORECASE
        ):

            warnings.append(
                "fatalities_vs_casualties"
            )

    protected_names = [
        (
            r"\bBabiš\w*",
            r"\bBabi[sš]\w*"
        ),
        (
            r"\bOkamur\w*",
            r"\bOkamur\w*"
        ),
        (
            r"\bPlag[ayoviě]+\b",
            r"\bPlaga\b"
        ),
        (
            r"\bAgrofert\w*",
            r"\bAgrofert\w*"
        ),
        (
            r"\bDukovan\w*",
            r"\bDukovan\w*"
        ),
        (
            r"\bTemelín\w*",
            r"\bTemel[ií]n\w*"
        ),
        (
            r"\bZelensk\w*",
            r"\bZelensk\w*"
        ),
    ]

    for source_pattern, target_pattern in protected_names:

        if re.search(
            source_pattern,
            source,
            re.IGNORECASE
        ) and not re.search(
            target_pattern,
            target,
            re.IGNORECASE
        ):

            warnings.append(
                "missing_expected_proper_name"
            )

    if re.search(
        r"\bUkrajin\w*",
        source,
        re.IGNORECASE
    ) and not re.search(
        r"\bUkrain\w*",
        target,
        re.IGNORECASE
    ):

        warnings.append(
            "missing_ukraine_reference"
        )

    if (
        "Lisbon strategy" in target.lower()
        and not re.search(
            r"\blisabon\w*",
            source,
            re.IGNORECASE
        )
    ):

        warnings.append(
            "unexpected_lisbon_strategy_reference"
        )

    return sorted(set(warnings))


# ============================================================
# ARTICLE TRANSLATION
# ============================================================

def needs_translation(article):

    if article.get("source_language") == "en":
        return False

    if not article.get("relevant"):
        return False

    if (
        article.get("translation_status") == "completed"
        and article.get("translation_version")
        == TRANSLATION_VERSION
    ):

        return False

    if (
        article.get("translation_attempts", 0)
        >= MAX_TRANSLATION_ATTEMPTS
    ):

        return False

    return True


def cache_valid_translation(
    source,
    target,
    cache
):

    if not source:
        return

    key = translation_cache_key(source)

    cache["entries"][key] = {
        "translation": target,
        "created_at_utc": utc_string(),
    }


def translate_article_group(
    articles,
    cache,
    usage
):

    texts = []

    for article in articles:

        texts.extend([
            article["title_raw"],
            article["lead_raw"],
        ])

    translations = translate_texts(
        texts,
        cache,
        usage
    )

    for index, article in enumerate(articles):

        title = translations[index * 2]
        lead = translations[index * 2 + 1]

        title_warnings = validate_translation(
            article["title_raw"],
            title
        )

        lead_warnings = validate_translation(
            article["lead_raw"],
            lead
        )

        warnings = sorted(
            set(
                [
                    f"title:{warning}"
                    for warning in title_warnings
                ]
                +
                [
                    f"lead:{warning}"
                    for warning in lead_warnings
                ]
            )
        )

        article["title_en"] = title
        article["lead_en"] = lead

        article["translation_attempts"] = (
            article.get("translation_attempts", 0)
            + 1
        )

        article["translation_version"] = (
            TRANSLATION_VERSION
        )

        article["validation_version"] = (
            VALIDATION_VERSION
        )

        article["translation_provider"] = (
            "Microsoft Azure Translator"
        )

        article["translation_method"] = (
            "whole_headline_and_lead"
        )

        article["translated_at_utc"] = utc_string()

        article["translation_warnings"] = warnings

        article["translation_error"] = None

        if warnings:

            article["translation_status"] = (
                "review_required"
            )

            print(
                f"[REVIEW] {article['title_raw']}"
            )

            print(
                f"[REVIEW] {', '.join(warnings)}"
            )

        else:

            article["translation_status"] = (
                "completed"
            )

        if not title_warnings:

            cache_valid_translation(
                article["title_raw"],
                title,
                cache
            )

        if not lead_warnings:

            cache_valid_translation(
                article["lead_raw"],
                lead,
                cache
            )


# ============================================================
# OUTPUT FEEDS
# ============================================================

def publishable(article):

    return (
        article.get("translation_status")
        in ("completed", "not_required")
        and bool(article.get("title_en"))
    )


def write_feed(filename, articles, topic=None):

    payload = {
        "country": COUNTRY,
        "country_code": COUNTRY_CODE,
        "updated_at_utc": utc_string(),
    }

    if topic:
        payload["topic"] = topic

    payload["count"] = len(articles)
    payload["articles"] = articles

    atomic_json_write(
        DATA_DIR / filename,
        payload
    )


def write_output_files(archive):

    active = active_articles(archive)

    published = [
        article
        for article in active
        if publishable(article)
        and article.get("relevant")
    ]

    write_feed(
        "latest.json",
        published
    )

    for topic in TOPICS:

        topic_articles = [
            article
            for article in published
            if topic in article.get("topics", [])
        ]

        write_feed(
            f"{topic}.json",
            topic_articles,
            topic
        )

    review_articles = [
        article
        for article in active
        if article.get("translation_status")
        == "review_required"
    ]

    write_feed(
        "review_required.json",
        review_articles
    )

    pending = [
        article
        for article in active
        if article.get("relevant")
        and article.get("translation_status")
        in ("pending", "failed")
    ]

    write_feed(
        "pending.json",
        pending
    )

    print(
        f"[OUTPUT] Published articles: "
        f"{len(published)}"
    )

    for topic in TOPICS:

        count = sum(
            topic in article.get("topics", [])
            for article in published
        )

        print(
            f"[OUTPUT] {topic}: {count}"
        )

    print(
        f"[OUTPUT] Review required: "
        f"{len(review_articles)}"
    )

    print(
        f"[OUTPUT] Pending: {len(pending)}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    start = time.perf_counter()

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print("=" * 70)
    print("CZECH MEDIA COLLECTOR V4")
    print("Microsoft Azure Translator")
    print("=" * 70)

    print(
        f"[CONFIG] Retention: {KEEP_HOURS} hours"
    )

    print(
        f"[CONFIG] Azure region: {AZURE_REGION}"
    )

    print(
        f"[CONFIG] Monthly safety limit: "
        f"{MONTHLY_CHARACTER_LIMIT:,} characters"
    )

    archive = load_archive()
    cache = load_cache()
    usage = load_usage()

    print(
        f"[ARCHIVE] Existing articles: "
        f"{len(archive)}"
    )

    print(
        f"[CACHE] Existing translations: "
        f"{len(cache.get('entries', {}))}"
    )

    print(
        f"[USAGE] Characters used this month: "
        f"{used_characters(usage):,}"
    )

    failures = collect_articles(archive)

    if failures == len(SOURCES):

        print(
            "[WARN] All RSS sources failed."
        )

    removed = prune_archive(archive)

    print(
        f"[ARCHIVE] Expired historical articles "
        f"removed: {removed}"
    )

    for article in active_articles(archive):

        if (
            article.get("classification_version")
            != CLASSIFICATION_VERSION
        ):

            classify_topics(article)

        if (
            article.get("relevant")
            and article.get("source_language") == "cs"
            and article.get("translation_status")
            == "not_relevant"
        ):

            article["translation_status"] = "pending"

    save_archive(archive)

    queue = [
        article
        for article in active_articles(archive)
        if needs_translation(article)
    ]

    print(
        f"[QUEUE] Articles requiring translation: "
        f"{len(queue)}"
    )

    processed = 0

    for offset in range(
        0,
        len(queue),
        TRANSLATION_BATCH_SIZE
    ):

        group = queue[
            offset:
            offset + TRANSLATION_BATCH_SIZE
        ]

        try:

            translate_article_group(
                group,
                cache,
                usage
            )

            processed += len(group)

            save_archive(archive)
            save_cache(cache)

        except TranslationBudgetExceeded as error:

            print(
                f"[BUDGET] {error}"
            )

            break

        except TranslationUnavailable as error:

            print(
                f"[WARN] Translation unavailable: "
                f"{error}"
            )

            break

        except Exception as error:

            print(
                f"[WARN] Translation processing error: "
                f"{type(error).__name__}"
            )

            break

    save_archive(archive)
    save_cache(cache)

    write_output_files(archive)

    elapsed = time.perf_counter() - start

    active = active_articles(archive)

    statuses = Counter(
        article.get("translation_status", "unknown")
        for article in active
    )

    print("=" * 70)
    print("FINAL STATISTICS")
    print("=" * 70)

    print(
        f"[STATS] Archive articles: {len(archive)}"
    )

    print(
        f"[STATS] Active articles: {len(active)}"
    )

    print(
        f"[STATS] Relevant active articles: "
        f"{sum(a.get('relevant', False) for a in active)}"
    )

    print(
        f"[STATS] Articles processed: {processed}"
    )

    for status, count in statuses.items():

        print(
            f"[STATUS] {status}: {count}"
        )

    print(
        f"[USAGE] Monthly characters: "
        f"{used_characters(usage):,} / "
        f"{MONTHLY_CHARACTER_LIMIT:,}"
    )

    print(
        f"[CACHE] Stored translations: "
        f"{len(cache.get('entries', {}))}"
    )

    print(
        f"[STATS] Runtime: {elapsed:.2f} seconds"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()
