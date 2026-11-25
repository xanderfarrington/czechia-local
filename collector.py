import os
import re
import json
import time
import html
import hashlib
import calendar
import unicodedata

from collections import Counter, deque
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

COLLECTOR_VERSION = "4.1"

KEEP_HOURS = 48
ARCHIVE_RETENTION_DAYS = 14
CACHE_RETENTION_DAYS = 14

TRANSLATION_VERSION = 5
CLASSIFICATION_VERSION = 5
VALIDATION_VERSION = 5

MONTHLY_CHARACTER_LIMIT = int(
    os.getenv("MONTHLY_CHARACTER_LIMIT", "1800000")
)

TRANSLATION_BATCH_SIZE = int(
    os.getenv("TRANSLATION_BATCH_SIZE", "25")
)

MAX_TRANSLATION_ATTEMPTS = 3

AZURE_REQUEST_TIMEOUT = 60
RSS_REQUEST_TIMEOUT = 30

AZURE_MAX_REQUEST_CHARACTERS = 40000
AZURE_MAX_REQUEST_ITEMS = 50

AZURE_CHARACTERS_PER_MINUTE = 25000

AZURE_TRANSLATOR_KEY = os.getenv(
    "AZURE_TRANSLATOR_KEY", ""
)

AZURE_TRANSLATOR_REGION = os.getenv(
    "AZURE_TRANSLATOR_REGION", ""
)

AZURE_ENDPOINT = (
    "https://api.cognitive.microsofttranslator.com/translate"
)

DATA_DIR = Path("data")

ARCHIVE_PATH = DATA_DIR / "articles_archive.json"
CACHE_PATH = DATA_DIR / "translation_cache.json"
USAGE_PATH = DATA_DIR / "api_usage.json"

TOPICS = [
    "diplomacy",
    "military",
    "energy",
    "economy",
    "domestic_politics",
    "foreign_affairs",
    "local_events",
]

TOPIC_THRESHOLDS = {
    "diplomacy": 6,
    "military": 6,
    "energy": 6,
    "economy": 6,
    "domestic_politics": 6,
    "foreign_affairs": 6,
    "local_events": 6,
}

USER_AGENT = (
    "Mozilla/5.0 "
    "(compatible; CzechMediaCollector/4.1)"
)

SESSION = requests.Session()

SESSION.headers.update({
    "User-Agent": USER_AGENT
})

REQUEST_HISTORY = deque()


# ============================================================
# RSS SOURCES
# ============================================================

SOURCES = [

    {
        "name": "iROZHLAS - Domestic",
        "url": (
            "https://www.irozhlas.cz/rss/"
            "irozhlas/section/zpravy-domov"
        ),
        "language": "cs",
    },

    {
        "name": "iROZHLAS - Economy",
        "url": (
            "https://www.irozhlas.cz/rss/"
            "irozhlas/section/ekonomika"
        ),
        "language": "cs",
    },

    {
        "name": "České noviny / ČTK - Czech Republic",
        "url": (
            "https://www.ceskenoviny.cz/"
            "sluzby/rss/cr.php"
        ),
        "language": "cs",
    },

    {
        "name": "České noviny / ČTK - Economy",
        "url": (
            "https://www.ceskenoviny.cz/"
            "sluzby/rss/ekonomika.php"
        ),
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
        "url": (
            "https://servis.idnes.cz/"
            "rss.aspx?c=zpravodaj"
        ),
        "language": "cs",
    },

    {
        "name": "Radio Prague International (EN)",
        "url": "https://english.radio.cz/rcz-rss/en",
        "language": "en",
    },

]


# ============================================================
# CLASSIFICATION RULES
#
# Format:
# (
#     concept,
#     strength,
#     Czech regex,
#     English regex
# )
#
# Strength:
# 3 = strong
# 2 = moderate
# 1 = supporting
# ============================================================

TOPIC_RULES = {

    "diplomacy": [

        (
            "diplomatic_relations", 3,
            r"\bdiplomac\w*|\bdiplomatick\w*\s+vztah\w*",
            r"\bdiplomacy\b|\bdiplomatic relations\b"
        ),

        (
            "ambassadors", 3,
            r"\bvelvyslan\w*",
            r"\bambassadors?\b"
        ),

        (
            "embassies", 3,
            r"\bvelvyslanectv\w*|\bambasád\w*",
            r"\bembass(?:y|ies)\b"
        ),

        (
            "peace_negotiations", 3,
            r"\bmírov\w*\s+jednán\w*|\bmírov\w*\s+rozhovor\w*",
            r"\bpeace talks\b|\bpeace negotiations\b"
        ),

        (
            "ceasefire", 3,
            r"\bpříměř\w*",
            r"\bceasefires?\b"
        ),

        (
            "bilateral_relations", 3,
            r"\bbilateráln\w*|\bdvoustrann\w*\s+vztah\w*",
            r"\bbilateral relations\b|\bbilateral talks\b"
        ),

        (
            "international_summits", 3,
            r"\bsummit\w*",
            r"\bsummits?\b"
        ),

        (
            "foreign_ministers", 3,
            r"\bministr\w*\s+zahranič\w*",
            r"\bforeign ministers?\b"
        ),

        (
            "official_visits", 2,
            r"\bstátn\w*\s+návštěv\w*|\bdiplomatick\w*\s+návštěv\w*",
            r"\bstate visits?\b|\bdiplomatic visits?\b"
        ),

        (
            "negotiations", 1,
            r"\bjednán\w*|\bvyjednáv\w*",
            r"\bnegotiations?\b|\btalks\b"
        ),

        (
            "international_agreements", 3,
            r"\bmezinárodn\w*\s+dohod\w*",
            r"\binternational agreements?\b"
        ),

    ],

    "military": [

        (
            "armed_forces", 3,
            r"\barmád\w*|\bozbrojen\w*\s+sil\w*",
            r"\barmed forces\b|\barm(?:y|ies)\b"
        ),

        (
            "military_activity", 3,
            r"\bvojensk\w*|\bvoják\w*",
            r"\bmilitary\b|\bsoldiers?\b"
        ),

        (
            "nato", 3,
            r"\bNATO\b|\bSeveroatlantick\w*\s+alianc\w*",
            r"\bNATO\b|\bNorth Atlantic Treaty Organization\b"
        ),

        (
            "defense_policy", 3,
            r"\bobrann\w*\s+(?:politik\w*|strategi\w*|rozpoč\w*|průmysl\w*)",
            r"\bdefen[cs]e (?:policy|strategy|budget|industry)\b"
        ),

        (
            "defense_spending", 3,
            r"\bvýdaj\w*\s+na\s+obran\w*",
            r"\bdefen[cs]e spending\b|\bmilitary expenditure\b"
        ),

        (
            "defense_ministry", 3,
            r"\bministerstv\w*\s+obran\w*|\bministr\w*\s+obran\w*",
            r"\bministry of defen[cs]e\b|\bdefen[cs]e minister\b"
        ),

        (
            "military_procurement", 3,
            r"\bvojensk\w*\s+zakázk\w*|\bnákup\w*\s+zbran\w*",
            r"\bmilitary procurement\b|\bweapons procurement\b"
        ),

        (
            "weapons", 2,
            r"\bzbraň\w*|\bzbran[ěí]\b|\bmunic\w*",
            r"\bweapons?\b|\bammunition\b|\bmunitions?\b"
        ),

        (
            "missiles", 3,
            r"\braketov\w*\s+útok\w*|\bbalistick\w*\s+střel\w*",
            r"\bballistic missiles?\b|\bmissile attacks?\b"
        ),

        (
            "air_defense", 3,
            r"\bprotivzdušn\w*\s+obran\w*",
            r"\bair defen[cs]e\b"
        ),

        (
            "military_drones", 3,
            r"\bbojov\w*\s+dron\w*|\bvojensk\w*\s+dron\w*|\bbezpilotn\w*\s+bojov\w*",
            r"\bmilitary drones?\b|\bcombat drones?\b|\barmed drones?\b"
        ),

        (
            "generic_drones", 1,
            r"\bdron\w*|\bbezpilotn\w*\s+letoun\w*",
            r"\bdrones?\b|\bunmanned aircraft\b"
        ),

        (
            "armored_vehicles", 3,
            r"\bbojov\w*\s+vozidl\w*|\bobrněn\w*\s+vozidl\w*",
            r"\barmou?red vehicles?\b|\binfantry fighting vehicles?\b"
        ),

        (
            "military_operations", 3,
            r"\bvojensk\w*\s+operac\w*|\bostřelov\w*",
            r"\bmilitary operations?\b|\bshelling\b"
        ),

        (
            "warfare", 3,
            r"\bhybridn\w*\s+vál[ck]\w*|\bválečn\w*\s+operac\w*",
            r"\bhybrid warfare\b|\bmilitary warfare\b"
        ),

        (
            "intelligence", 3,
            r"\bzpravodajsk\w*\s+služb\w*|\bvojensk\w*\s+zpravodajstv\w*|\bšpionáž\w*",
            r"\bintelligence services?\b|\bmilitary intelligence\b|\bespionage\b"
        ),

        (
            "mobilization", 3,
            r"\bmobilizac\w*|\bvojensk\w*\s+záloh\w*",
            r"\bmilitary mobilization\b|\bmilitary reserves\b"
        ),

        (
            "military_exercises", 3,
            r"\bvojensk\w*\s+cvičen\w*",
            r"\bmilitary exercises?\b"
        ),

        (
            "allied_forces", 3,
            r"\bspojeneck\w*\s+(?:vojsk\w*|jednotk\w*)",
            r"\ballied forces\b|\ballied troops\b"
        ),

    ],

    "energy": [

        (
            "nuclear_energy", 3,
            r"\bjadern\w*\s+(?:energetik\w*|elektrár\w*|reaktor\w*)",
            r"\bnuclear energy\b|\bnuclear power\b|\bnuclear reactors?\b"
        ),

        (
            "czech_nuclear_plants", 3,
            r"\bDukovan\w*|\bTemelín\w*",
            r"\bDukovany\b|\bTemelín\b|\bTemelin\b"
        ),

        (
            "electricity", 3,
            r"\belektřin\w*|\belektrárn\w*",
            r"\belectricity\b|\bpower plants?\b"
        ),

        (
            "energy_prices", 3,
            r"\bcen\w*\s+energi\w*|\bcen\w*\s+elektřin\w*",
            r"\benergy prices?\b|\belectricity prices?\b"
        ),

        (
            "energy_policy", 3,
            r"\benergetick\w*\s+(?:politik\w*|strategi\w*|bezpečnost\w*)",
            r"\benergy policy\b|\benergy strategy\b|\benergy security\b"
        ),

        (
            "oil", 3,
            r"\brop[ayěuou]\b|\bro p n\w*|\bropovod\w*",
            r"\bcrude oil\b|\boil prices?\b|\boil pipelines?\b"
        ),

        (
            "natural_gas", 3,
            r"\bzemn\w*\s+plyn\w*|\bplynovod\w*",
            r"\bnatural gas\b|\bgas pipelines?\b"
        ),

        (
            "fuel_prices", 3,
            r"\bcen\w*\s+pohonn\w*\s+hmot\w*|\bcen\w*\s+benz[ií]n\w*|\bcen\w*\s+naft\w*",
            r"\bfuel prices?\b|\bpetrol prices?\b|\bdiesel prices?\b"
        ),

        (
            "coal", 3,
            r"\buhlí\b|\buheln\w*\s+elektrár\w*",
            r"\bcoal\b|\bcoal-fired power\b"
        ),

        (
            "renewable_energy", 3,
            r"\bobnoviteln\w*\s+zdroj\w*|\bsolárn\w*\s+energi\w*",
            r"\brenewable energy\b|\bsolar power\b|\brenewables\b"
        ),

        (
            "wind_energy", 3,
            r"\bvětrn\w*\s+elektrár\w*",
            r"\bwind farms?\b|\bwind power\b"
        ),

        (
            "hydropower", 3,
            r"\bvodn\w*\s+elektrár\w*|\bpřečerpávac\w*",
            r"\bhydropower\b|\bpumped storage\b|\bhydroelectric\b"
        ),

        (
            "district_heating", 3,
            r"\bteplárn\w*|\bteplárenstv\w*",
            r"\bdistrict heating\b|\bheating plants?\b"
        ),

        (
            "power_grid", 3,
            r"\belektrick\w*\s+sít\w*|\bpřenosov\w*\s+soustav\w*",
            r"\bpower grids?\b|\belectricity grids?\b|\btransmission networks?\b"
        ),

        (
            "energy_storage", 3,
            r"\bakumulac\w*\s+energi\w*|\bbateriov\w*\s+úložišt\w*",
            r"\benergy storage\b|\bbattery storage\b"
        ),

        (
            "critical_minerals", 3,
            r"\bkritick\w*\s+surovin\w*|\bvzácn\w*\s+zemin\w*|\blithium\b|\blithi\w*",
            r"\bcritical minerals\b|\brare earths\b|\blithium\b"
        ),

        (
            "refining", 3,
            r"\brafineri\w*|\brafinéri\w*",
            r"\brefiner(?:y|ies)\b"
        ),

        (
            "energy_regulation", 3,
            r"\bEnergetick\w*\s+regulačn\w*\s+úřad\w*",
            r"\bEnergy Regulatory Office\b"
        ),

    ],

    "economy": [

        (
            "macroeconomics", 3,
            r"\bmakroekonom\w*|\bHDP\b|\bhrub\w*\s+domác\w*\s+produkt\w*",
            r"\bmacroeconom\w*|\bGDP\b|\bgross domestic product\b"
        ),

        (
            "gdp_growth", 3,
            r"\bekonomik\w*.{0,70}\b(?:růst\w*|vzrost\w*|pokles\w*|kles\w*|zrychl\w*)|\brůst\w*\s+ekonomik\w*",
            r"\beconomic growth\b|\beconomy grew\b|\bGDP growth\b|\beconomic contraction\b"
        ),

        (
            "inflation", 3,
            r"\binflac\w*|\bspotřebitelsk\w*\s+cen\w*",
            r"\binflation\b|\bconsumer prices\b"
        ),

        (
            "interest_rates", 3,
            r"\búrokov\w*\s+sazb\w*",
            r"\binterest rates?\b"
        ),

        (
            "central_banking", 3,
            r"\bČNB\b|\bcentráln\w*\s+bank\w*|\bČesk\w*\s+národn\w*\s+bank\w*",
            r"\bcentral banks?\b|\bCzech National Bank\b|\bCNB\b"
        ),

        (
            "government_budget", 3,
            r"\bstátn\w*\s+rozpoč\w*",
            r"\bstate budget\b|\bgovernment budget\b|\bnational budget\b"
        ),

        (
            "public_finances", 3,
            r"\bveřejn\w*\s+(?:rozpoč\w*|financ\w*)|\bfiskáln\w*|\brozpočtov\w*\s+schod\w*",
            r"\bpublic finances\b|\bfiscal policy\b|\bbudget deficit\b"
        ),

        (
            "eu_budget", 3,
            r"\bunijn\w*\s+rozpoč\w*|\brozpoč\w*\s+EU\b|\brozpoč\w*\s+Evropsk\w*\s+uni\w*|\bEU\b.{0,60}\butrat\w*",
            r"\bEU budget\b|\bEuropean Union budget\b|\bEU spending\b"
        ),

        (
            "government_debt", 3,
            r"\bstátn\w*\s+dluh\w*|\bveřejn\w*\s+zadlužen\w*",
            r"\bgovernment debt\b|\bpublic debt\b|\bsovereign debt\b"
        ),

        (
            "taxation", 3,
            r"\bdaňov\w*|\bdaně\b|\bDPH\b",
            r"\btaxation\b|\btax policy\b|\bVAT\b"
        ),

        (
            "unemployment", 3,
            r"\bnezaměstnan\w*|\brekvalifikac\w*",
            r"\bunemployment\b|\bunemployment benefits\b|\bretraining\b"
        ),

        (
            "labor_market", 3,
            r"\bpracovn\w*\s+trh\w*|\bzákoník\w*\s+prác\w*|\bšvarcsystém\w*",
            r"\blabou?r market\b|\blabou?r code\b|\bdisguised employment\b"
        ),

        (
            "wages", 3,
            r"\bmzd[ayouě]\b|\bmzdov\w*|\bminimáln\w*\s+mzd\w*",
            r"\bwages\b|\bminimum wage\b|\bwage growth\b"
        ),

        (
            "pensions", 2,
            r"\bdůchodov\w*\s+(?:reform\w*|systém\w*|výdaj\w*)",
            r"\bpension reform\b|\bpension system\b|\bpension spending\b"
        ),

        (
            "tariffs", 3,
            r"\bcla\b|\bcel\b|\bclům\b|\bceln\w*\s+sazb\w*",
            r"\btariffs?\b|\bimport duties\b"
        ),

        (
            "international_trade", 3,
            r"\bmezinárodn\w*\s+obchod\w*|\bzahraničn\w*\s+obchod\w*|\bobchodn\w*\s+bilanc\w*",
            r"\binternational trade\b|\bforeign trade\b|\btrade balance\b"
        ),

        (
            "imports_exports", 3,
            r"\bdovoz\w*|\bvývoz\w*|\bexport\w*|\bimport\w*",
            r"\bimports?\b|\bexports?\b"
        ),

        (
            "industrial_policy", 3,
            r"\bprůmyslov\w*\s+politik\w*|\bprůmyslov\w*\s+výrob\w*",
            r"\bindustrial policy\b|\bindustrial production\b"
        ),

        (
            "manufacturing", 2,
            r"\bvýrobn\w*\s+kapacit\w*|\bzpracovatelsk\w*\s+průmysl\w*",
            r"\bmanufacturing capacity\b|\bmanufacturing sector\b"
        ),

        (
            "corporate_investment", 3,
            r"\bmiliardov\w*\s+investic\w*|\binvestičn\w*\s+projekt\w*",
            r"\bmajor investments?\b|\binvestment projects?\b"
        ),

        (
            "mergers_acquisitions", 3,
            r"\bakvizic\w*|\bfúz[ei]\b",
            r"\bacquisitions?\b|\bmergers?\b"
        ),

        (
            "capital_markets", 3,
            r"\bburz[ayěu]\b|\bakciov\w*\s+trh\w*|\bkapitálov\w*\s+trh\w*",
            r"\bstock exchanges?\b|\bstock markets?\b|\bcapital markets?\b"
        ),

        (
            "commodities", 3,
            r"\bcen\w*\s+zlat\w*|\bcen\w*\s+stříbr\w*|\bkomodit\w*",
            r"\bgold prices?\b|\bsilver prices?\b|\bcommodit(?:y|ies)\b"
        ),

        (
            "agricultural_subsidies", 3,
            r"\bzemědělsk\w*\s+dotac\w*|\bzemědělc\w*.{0,80}\bdotac\w*",
            r"\bagricultural subsidies\b|\bfarm subsidies\b"
        ),

        (
            "housing_market", 3,
            r"\bcen\w*\s+byt\w*|\btrh\w*\s+s\s+bydlen\w*|\bnájm\w*.{0,40}\bzdraž\w*",
            r"\bhousing market\b|\bproperty prices\b|\brental prices\b|\brents rose\b"
        ),

        (
            "automotive_market", 3,
            r"\bprodej\w*\s+nov\w*\s+aut\w*|\bregistrac\w*\s+nov\w*\s+voz\w*",
            r"\bnew car sales\b|\bvehicle registrations\b|\bnew car registrations\b"
        ),

        (
            "economic_confidence", 3,
            r"\bdůvěr\w*\s+v\s+(?:česk\w*\s+)?ekonomik\w*",
            r"\beconomic confidence\b|\bconsumer confidence\b|\bbusiness confidence\b"
        ),

        (
            "currency", 1,
            r"\bkorun\w*|\bKč\b|\beur\w*|\bdolar\w*",
            r"\bCZK\b|\beuros?\b|\bdollars?\b|\bcrowns?\b"
        ),

        (
            "generic_budget", 1,
            r"\brozpoč\w*",
            r"\bbudgets?\b"
        ),

    ],

    "domestic_politics": [

        (
            "parliament", 3,
            r"\bsněmovn\w*|\bsenát\w*|\bparlament\w*|\bposlanc\w*",
            r"\bChamber of Deputies\b|\bSenate\b|\bparliament\b|\bMPs?\b"
        ),

        (
            "government", 2,
            r"\bvlád\w*|\bministerstv\w*",
            r"\bgovernment\b|\bministr(?:y|ies)\b"
        ),

        (
            "legislation", 3,
            r"\bzákon\w*|\bpozměňovac\w*\s+návrh\w*|\bnovel\w*\s+zákon\w*",
            r"\blegislation\b|\bbill\b|\bdraft law\b|\blegislative amendment\b"
        ),

        (
            "elections", 3,
            r"\bvolb\w*|\bvoleb\w*",
            r"\belections?\b|\belectoral\b"
        ),

        (
            "coalition", 3,
            r"\bkoalic\w*|\bopozic\w*",
            r"\bcoalition\b|\bpolitical opposition\b"
        ),

        (
            "political_parties", 3,
            r"(?-i:\b(?:ANO|ODS|SPD|STAN|TOP 09|KDU-ČSL|SOCDEM)\b)|\bPiráti\b|\bMotoristé\b",
            r"(?-i:\b(?:ANO|ODS|SPD|STAN|TOP 09|KDU-CSL|SOCDEM)\b)|\bPirate Party\b"
        ),

        (
            "czech_leaders", 3,
            r"\bBabiš\w*|\bOkamur\w*|\bFial\w*|\bPavl\w*\s+Petr\w*|\bPetr\w*\s+Pavl\w*",
            r"\bBabiš\b|\bBabis\b|\bOkamura\b|\bFiala\b|\bPetr Pavel\b"
        ),

        (
            "leadership", 1,
            r"\bprezident\w*|\bpremiér\w*|\bministr\w*",
            r"\bpresident\b|\bprime minister\b|\bministers?\b"
        ),

        (
            "corruption", 3,
            r"\bkorupc\w*|\bpolitick\w*\s+skandál\w*",
            r"\bcorruption\b|\bpolitical scandals?\b"
        ),

        (
            "public_broadcasting_policy", 3,
            r"\bfinancov\w*.{0,60}\bveřejnoprávn\w*|\bkoncesionářsk\w*\s+poplat\w*|\bveřejnoprávn\w*\s+médi\w*",
            r"\bpublic broadcasting funding\b|\blicen[cs]e fees\b|\bpublic service media\b"
        ),

        (
            "regulations", 3,
            r"\bvyhlášk\w*|\bnov\w*\s+povinnost\w*|\bzávazn\w*\s+pravidl\w*",
            r"\bgovernment decrees?\b|\bmandatory regulations\b|\bregulatory requirements\b"
        ),

        (
            "public_administration", 3,
            r"\bveřejn\w*\s+správ\w*|\bstátn\w*\s+správ\w*",
            r"\bpublic administration\b|\bstate administration\b"
        ),

        (
            "appointments", 3,
            r"\bjmenov\w*.{0,30}\bministr\w*|\bodvol\w*.{0,30}\bministr\w*",
            r"\bministerial appointments?\b|\bcabinet reshuffle\b"
        ),

        (
            "municipal_government", 3,
            r"\bmístostarost\w*|\bmagistrát\w*|\bradn[íice]\w*|\bzastupitelstv\w*",
            r"\bdeputy mayor\b|\bcity council\b|\bmunicipal council\b"
        ),

        (
            "constitutional_affairs", 3,
            r"\bústavn\w*\s+soud\w*|\bústavn\w*\s+kriz\w*",
            r"\bconstitutional court\b|\bconstitutional crisis\b"
        ),

        (
            "political_demonstrations", 3,
            r"\bprotivládn\w*\s+protest\w*|\bpolitick\w*\s+demonstrac\w*",
            r"\banti-government protests?\b|\bpolitical demonstrations?\b"
        ),

    ],

    "foreign_affairs": [

        (
            "international_relations", 3,
            r"\bmezinárodn\w*\s+vztah\w*|\bzahraničn\w*\s+politik\w*",
            r"\binternational relations\b|\bforeign policy\b"
        ),

        (
            "eu_policy", 3,
            r"\bEvropsk\w*\s+uni\w*|\bEvropsk\w*\s+komis\w*|\bEvropsk\w*\s+parlament\w*|\bEU\b",
            r"\bEuropean Union\b|\bEuropean Commission\b|\bEuropean Parliament\b|\bEU\b"
        ),

        (
            "ukraine", 3,
            r"\bUkrajin\w*|\bukrajinsk\w*",
            r"\bUkraine\b|\bUkrainian\b"
        ),

        (
            "russia", 2,
            r"\bRusko\b|\bRusk\w*\s+federac\w*|\bKreml\w*|\bPutin\w*",
            r"\bRussia\b|\bRussian Federation\b|\bKremlin\b|\bPutin\b"
        ),

        (
            "international_conflict", 3,
            r"\bválečn\w*\s+konflikt\w*|\bozbrojen\w*\s+konflikt\w*|\binvaz\w*",
            r"\barmed conflict\b|\bmilitary invasion\b|\binternational conflict\b"
        ),

        (
            "sanctions", 3,
            r"\bsankc\w*|\bembarg\w*",
            r"\bsanctions\b|\bembargo\b"
        ),

        (
            "foreign_elections", 3,
            r"\bprezidentsk\w*\s+volb\w*|\bparlamentn\w*\s+volb\w*",
            r"\bpresidential elections?\b|\bparliamentary elections?\b"
        ),

        (
            "international_trade", 3,
            r"\bobchodn\w*\s+válk\w*|\bmezinárodn\w*\s+obchod\w*",
            r"\btrade war\b|\binternational trade\b"
        ),

        (
            "political_purges", 3,
            r"\bpolitick\w*\s+čistk\w*|\bpolitbyr\w*",
            r"\bpolitical purges?\b|\bpolitburo\b"
        ),

        (
            "international_security", 3,
            r"\bmezinárodn\w*\s+bezpečnost\w*|\bgeopolit\w*",
            r"\binternational security\b|\bgeopolit\w*"
        ),

        (
            "terrorism", 3,
            r"\bterorism\w*|\bterorist\w*",
            r"\bterrorism\b|\bterrorist attacks?\b"
        ),

        (
            "migration_policy", 3,
            r"\bmigrační\w*\s+politik\w*|\bazylov\w*\s+politik\w*",
            r"\bmigration policy\b|\basylum policy\b"
        ),

        (
            "international_organizations", 3,
            r"\bOSN\b|\bRada\s+bezpečnosti\b",
            r"\bUnited Nations\b|\bUN Security Council\b"
        ),

        (
            "foreign_aid", 3,
            r"\bvojensk\w*\s+pomoc\w*|\bhumanitárn\w*\s+pomoc\w*|\bzahraničn\w*\s+pomoc\w*",
            r"\bmilitary aid\b|\bhumanitarian aid\b|\bforeign aid\b"
        ),

    ],

    "local_events": [

        (
            "public_transport", 3,
            r"\bveřejn\w*\s+doprav\w*|\bměstsk\w*\s+hromadn\w*\s+doprav\w*|\bMHD\b",
            r"\bpublic transport\b|\bpublic transit\b"
        ),

        (
            "transport_disruption", 3,
            r"\bvýluk\w*|\buzavírk\w*|\bdopravn\w*\s+omezen\w*",
            r"\btransport disruptions?\b|\broad closures?\b|\brail disruptions?\b"
        ),

        (
            "infrastructure", 3,
            r"\bstavb\w*\s+most\w*|\bnov\w*\s+lávk\w*|\bstavb\w*\s+dálnic\w*",
            r"\bbridge construction\b|\bnew pedestrian bridge\b|\bmotorway construction\b"
        ),

        (
            "severe_weather", 3,
            r"\bsněhov\w*\s+nadílk\w*|\bsiln\w*\s+sněžen\w*|\bmeteorolog\w*\s+výstrah\w*|\bextrémn\w*\s+počas\w*",
            r"\bheavy snowfall\b|\bsevere weather\b|\bweather warnings?\b"
        ),

        (
            "emergency_warning", 3,
            r"\bvýstrah\w*.{0,50}\bsněh\w*|\bvýstrah\w*.{0,50}\bvítr\b|\bČHMÚ\b",
            r"\bsnow warnings?\b|\bstorm warnings?\b|\bemergency warnings?\b"
        ),

        (
            "natural_disasters", 3,
            r"\bpovodn\w*|\bzáplav\w*|\bzemětřesen\w*|\blesn\w*\s+požár\w*",
            r"\bfloods?\b|\bflooding\b|\bearthquakes?\b|\bwildfires?\b"
        ),

        (
            "public_emergency", 3,
            r"\bevakuac\w*|\bkrizov\w*\s+stav\w*|\bnouzov\w*\s+stav\w*",
            r"\bevacuation\b|\bstate of emergency\b"
        ),

        (
            "infrastructure_disruption", 3,
            r"\bhavarijn\w*\s+stav\w*|\buzavřen\w*\s+most\w*|\buzavřen\w*\s+tunel\w*",
            r"\binfrastructure closures?\b|\bbridge closures?\b|\btunnel closures?\b"
        ),

        (
            "rail_accidents", 3,
            r"\bsrážk\w*\s+vlak\w*|\bželezničn\w*\s+nehod\w*",
            r"\btrain collisions?\b|\brail accidents?\b"
        ),

        (
            "demonstrations", 3,
            r"\bdemonstrac\w*|\bprotestn\w*\s+pochod\w*",
            r"\bdemonstrations?\b|\bprotest marches?\b"
        ),

    ],

}


# ============================================================
# CONTEXTUAL CLASSIFICATION FILTERS
# ============================================================

CZECH_CONTEXT = re.compile(

    r"\bČesko\b|\bČesk\w*|\bČR\b|\bPra[hz]\w*|"
    r"\bBrn\w*|\bOstrav\w*|\bPlz[eň]\w*|"
    r"\bOlomouc\w*|\bLiberec\w*|\bZlín\w*|"
    r"\bPardubic\w*|\bJihlav\w*|"
    r"\bBabiš\w*|\bOkamur\w*|\bFial\w*|"
    r"\bCzech\b|\bCzechia\b|\bPrague\b|"
    r"\bBohemia\b|\bMoravia\b|"
    r"\bČNB\b|\bČTK\b|"
    r"(?-i:\b(?:ANO|ODS|STAN|SPD|KDU-ČSL)\b)",

    re.IGNORECASE

)

MILITARY_CONTEXT = re.compile(

    r"\bvojensk\w*|\barmád\w*|\bvoják\w*|"
    r"\bNATO\b|\bobrann\w*|\bmunic\w*|"
    r"\bbojov\w*|\bfrontov\w*|\bostřelov\w*|"
    r"\bUkrajin\w*|\bválečn\w*|"
    r"\bmilitary\b|\barmy\b|\bsoldiers?\b|"
    r"\bdefen[cs]e\b|\bcombat\b|\bwarfare\b|"
    r"\bmissiles?\b|\barmed forces\b",

    re.IGNORECASE

)

ARCHAEOLOGY_CONTEXT = re.compile(

    r"\barcheolog\w*|\bdob[ayěu]\s+bronzov\w*|"
    r"\bpravěk\w*|\bstředověk\w*|"
    r"\barcheolog\w*|\barchaeolog\w*|"
    r"\bBronze Age\b|\bStone Age\b|"
    r"\bancient settlements?\b",

    re.IGNORECASE

)

CONSUMER_DRONE_CONTEXT = re.compile(

    r"\bšampon\w*|\bšampón\w*|\bdoručov\w*|"
    r"\bdoručen\w*|\bzásilk\w*|\bspotřebitel\w*|"
    r"\bshampoo\b|\bconsumer\b|"
    r"\bdelivery\b|\bdeliveries\b",

    re.IGNORECASE

)

MEDIA_AWARD_CONTEXT = re.compile(

    r"\bpodcast\w*|\boceněn\w*|\bvyhrál\w*|"
    r"\bmediáln\w*\s+cenu\b|"
    r"\bpodcast\b|\bmedia awards?\b",

    re.IGNORECASE

)

DIPLOMATIC_CONTEXT = re.compile(

    r"\bdiplomac\w*|\bdiplomat\w*|"
    r"\bvelvyslan\w*|\bzahraničn\w*|"
    r"\bsummit\w*|\bmírov\w*|"
    r"\bmezinárodn\w*\s+vztah\w*|"
    r"\bforeign minister\b|\bdiplomatic\b|"
    r"\bembassy\b|\bambassador\b|"
    r"\bpeace talks\b|\bbilateral\b|\bsummit\b",

    re.IGNORECASE

)


# ============================================================
# EXCEPTIONS
# ============================================================

class TranslationBudgetExceeded(Exception):
    pass


class TranslationUnavailable(Exception):
    pass


# ============================================================
# GENERAL UTILITIES
# ============================================================

def utc_now():

    return datetime.now(timezone.utc)


def iso_utc(value=None):

    if value is None:
        value = utc_now()

    return value.astimezone(
        timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha1(value):

    return hashlib.sha1(
        value.encode("utf-8")
    ).hexdigest()


def clean_text(value):

    if not value:
        return ""

    value = html.unescape(str(value))

    value = re.sub(
        r"<[^>]+>",
        " ",
        value
    )

    value = re.sub(
        r"\s+",
        " ",
        value
    ).strip()

    return unicodedata.normalize(
        "NFC",
        value
    )


def normalize_url(value):

    if not value:
        return ""

    parts = urlsplit(value.strip())

    query_parts = []

    for item in parts.query.split("&"):

        if not item:
            continue

        key = item.split("=")[0].lower()

        if key.startswith("utm_"):
            continue

        if key in {
            "fbclid",
            "gclid",
            "ref",
            "source",
            "campaign",
        }:
            continue

        query_parts.append(item)

    query = "&".join(query_parts)

    return urlunsplit((

        parts.scheme.lower(),
        parts.netloc.lower(),
        parts.path.rstrip("/"),
        query,
        ""

    ))


def parse_date(value):

    if not value:
        return None

    try:

        result = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

        if result.tzinfo is None:

            result = result.replace(
                tzinfo=timezone.utc
            )

        return result.astimezone(
            timezone.utc
        )

    except (ValueError, TypeError):

        pass

    try:

        result = parsedate_to_datetime(
            str(value)
        )

        if result.tzinfo is None:

            result = result.replace(
                tzinfo=timezone.utc
            )

        return result.astimezone(
            timezone.utc
        )

    except Exception:

        return None


def entry_date(entry):

    for field in (
        "published_parsed",
        "updated_parsed",
        "created_parsed"
    ):

        parsed = entry.get(field)

        if parsed:

            try:

                return datetime.fromtimestamp(
                    calendar.timegm(parsed),
                    tz=timezone.utc
                )

            except Exception:

                pass

    for field in (
        "published",
        "updated",
        "created",
        "date"
    ):

        result = parse_date(
            entry.get(field)
        )

        if result:

            return result

    return None


def read_json(path, default):

    if not path.exists():
        return default

    try:

        with path.open(
            "r",
            encoding="utf-8"
        ) as handle:

            return json.load(handle)

    except Exception as exc:

        print(
            f"[WARN] Could not read "
            f"{path.name}: {type(exc).__name__}"
        )

        return default


def write_json(path, data):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8"
    ) as handle:

        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2
        )

    temporary.replace(path)


# ============================================================
# TOPIC CLASSIFICATION
# ============================================================

def is_czech_context(article):

    text = (
        article.get("title_raw", "")
        + " "
        + article.get("lead_raw", "")
    )

    source = article.get("source", "")
    url = article.get("url", "")

    if CZECH_CONTEXT.search(text):
        return True

    if "Domestic" in source:
        return True

    if "Czech Republic" in source:
        return True

    if "/domaci" in url:
        return True

    if "/zpravy-domov" in url:
        return True

    if "/clanek/domaci-" in url:
        return True

    return False


def classify_topics(article):

    title = article.get("title_raw", "")
    lead = article.get("lead_raw", "")

    full_text = title + " " + lead

    language = article.get(
        "source_language",
        "cs"
    )

    czech_context = is_czech_context(
        article
    )

    scores = {}
    matched = {}
    details = {}

    for topic in TOPICS:

        scores[topic] = 0
        matched[topic] = []
        details[topic] = []

        if topic in {
            "domestic_politics",
            "local_events"
        } and not czech_context:

            continue

        for rule in TOPIC_RULES[topic]:

            concept, strength, cs, en = rule

            pattern = (
                cs if language == "cs" else en
            )

            if not pattern:
                continue

            title_match = bool(
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

            if not (
                title_match or lead_match
            ):

                continue

            points = (
                strength * 3
                if title_match
                else strength * 2
            )

            scores[topic] += points

            matched[topic].append(
                concept
            )

            details[topic].append({

                "concept": concept,
                "strength": strength,
                "headline": title_match,
                "lead": lead_match,
                "points": points,

            })

    # Ancient archaeology is not modern military news.
    if ARCHAEOLOGY_CONTEXT.search(
        full_text
    ):

        scores["military"] = 0

        matched["military"] = []
        details["military"] = []

    # Generic drones require genuine military context.
    if not MILITARY_CONTEXT.search(
        full_text
    ):

        weak_military_concepts = {
            "generic_drones",
            "weapons"
        }

        retained = [

            item
            for item in details["military"]

            if item["concept"] not in
            weak_military_concepts

        ]

        details["military"] = retained

        matched["military"] = [
            item["concept"]
            for item in retained
        ]

        scores["military"] = sum(
            item["points"]
            for item in retained
        )

    if (
        CONSUMER_DRONE_CONTEXT.search(
            full_text
        )

        and not MILITARY_CONTEXT.search(
            full_text
        )
    ):

        scores["military"] = 0
        matched["military"] = []
        details["military"] = []

    # Incidental diplomatic terminology is insufficient.
    if not DIPLOMATIC_CONTEXT.search(
        full_text
    ):

        retained = [

            item
            for item in details["diplomacy"]

            if item["concept"] not in {
                "official_visits",
                "negotiations"
            }

        ]

        details["diplomacy"] = retained

        matched["diplomacy"] = [
            item["concept"]
            for item in retained
        ]

        scores["diplomacy"] = sum(
            item["points"]
            for item in retained
        )

    # Media awards should not automatically be political.
    if MEDIA_AWARD_CONTEXT.search(
        full_text
    ):

        political_matches = set(
            matched["domestic_politics"]
        )

        if political_matches.issubset({

            "public_broadcasting_policy",
            "leadership",
            "government"

        }):

            scores["domestic_politics"] = 0

            matched["domestic_politics"] = []
            details["domestic_politics"] = []

    topics = []

    for topic in TOPICS:

        strong_match = any(

            item["strength"] == 3

            for item in details[topic]

        )

        if (
            scores[topic]
            >= TOPIC_THRESHOLDS[topic]

            and strong_match
        ):

            topics.append(topic)

    primary_topic = None

    if topics:

        primary_topic = max(

            topics,

            key=lambda topic: scores[topic]

        )

    return {

        "topic_scores": scores,
        "matched_keywords": matched,
        "classification_details": details,
        "topics": topics,
        "relevant": bool(topics),
        "primary_topic": primary_topic,
        "classification_version": (
            CLASSIFICATION_VERSION
        ),

    }


# ============================================================
# ARTICLE CREATION
# ============================================================

def make_article(source, entry):

    title = clean_text(
        entry.get("title")
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

    if not title or not url:

        return None

    if published is None:

        return None

    cutoff = utc_now() - timedelta(
        hours=KEEP_HOURS
    )

    if published < cutoff:

        return None

    if published > utc_now() + timedelta(
        hours=1
    ):

        return None

    article = {

        "id": sha1(url),

        "fingerprint": sha1(
            title + "\n" + lead
        ),

        "source": source["name"],
        "source_language": source["language"],

        "url": url,

        "published_utc": iso_utc(
            published
        ),

        "collected_at_utc": iso_utc(),

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

    article.update(
        classify_topics(article)
    )

    if source["language"] == "en":

        article["title_en"] = title
        article["lead_en"] = lead

        article["translation_status"] = (
            "not_required"
        )

    elif not article["relevant"]:

        article["translation_status"] = (
            "not_relevant"
        )

    return article


# ============================================================
# RSS COLLECTION
# ============================================================

def fetch_source(source):

    response = SESSION.get(

        source["url"],

        timeout=RSS_REQUEST_TIMEOUT

    )

    response.raise_for_status()

    feed = feedparser.parse(
        response.content
    )

    if feed.bozo and not feed.entries:

        raise RuntimeError(
            "RSS parsing failed"
        )

    articles = []

    for entry in feed.entries:

        article = make_article(
            source,
            entry
        )

        if article:

            articles.append(article)

    print(

        f"[RSS] {source['name']}: "

        f"{len(articles)} active articles"

    )

    return articles


def load_archive():

    payload = read_json(

        ARCHIVE_PATH,

        {
            "articles": []
        }

    )

    articles = payload.get(
        "articles",
        []
    )

    archive = {}

    for article in articles:

        if article.get("url"):

            archive[
                article["url"]
            ] = article

    return archive


def collect_articles(archive):

    counters = {

        "new": 0,
        "updated": 0,
        "unchanged": 0,
        "failed_sources": 0,

    }

    for source in SOURCES:

        try:

            articles = fetch_source(
                source
            )

        except Exception as exc:

            counters[
                "failed_sources"
            ] += 1

            print(

                f"[WARN] RSS failure: "

                f"{source['name']} | "

                f"{type(exc).__name__}"

            )

            continue

        for article in articles:

            url = article["url"]

            existing = archive.get(url)

            if existing is None:

                archive[url] = article

                counters["new"] += 1

            elif (

                existing.get("fingerprint")

                != article["fingerprint"]

            ):

                archive[url] = article

                counters["updated"] += 1

            else:

                counters["unchanged"] += 1

    print(
        f"[RSS] New articles: "
        f"{counters['new']}"
    )

    print(
        f"[RSS] Updated articles: "
        f"{counters['updated']}"
    )

    print(
        f"[RSS] Unchanged articles: "
        f"{counters['unchanged']}"
    )

    print(
        f"[RSS] Failed sources: "
        f"{counters['failed_sources']}"
    )

    return counters


def prune_archive(archive):

    cutoff = utc_now() - timedelta(
        days=ARCHIVE_RETENTION_DAYS
    )

    expired = []

    for url, article in archive.items():

        published = parse_date(
            article.get("published_utc")
        )

        if not published:

            expired.append(url)

        elif published < cutoff:

            expired.append(url)

    for url in expired:

        del archive[url]

    print(

        f"[ARCHIVE] Expired historical "

        f"articles removed: {len(expired)}"

    )


def get_active_articles(archive):

    cutoff = utc_now() - timedelta(
        hours=KEEP_HOURS
    )

    articles = []

    for article in archive.values():

        published = parse_date(
            article.get("published_utc")
        )

        if published and published >= cutoff:

            articles.append(article)

    return sorted(

        articles,

        key=lambda article: article.get(
            "published_utc",
            ""
        ),

        reverse=True

    )


# ============================================================
# TRANSLATION CACHE
# ============================================================

def translation_cache_key(text):

    return sha1(

        f"azure-v{TRANSLATION_VERSION}"

        f"|cs|en|{text}"

    )


def load_cache():

    payload = read_json(

        CACHE_PATH,

        {
            "provider": (
                "Microsoft Azure Translator"
            ),
            "entries": {}
        }

    )

    entries = payload.get(
        "entries",
        {}
    )

    cutoff = utc_now() - timedelta(
        days=CACHE_RETENTION_DAYS
    )

    retained = {}

    for key, entry in entries.items():

        if not isinstance(entry, dict):

            continue

        created = parse_date(

            entry.get("created_at_utc")

            or entry.get("translated_at_utc")

        )

        if created and created >= cutoff:

            retained[key] = entry

    print(

        f"[CACHE] Expired translations removed: "

        f"{len(entries) - len(retained)}"

    )

    return {

        "provider": (
            "Microsoft Azure Translator"
        ),

        "entries": retained,

    }


def cache_translation(cache, raw, translated):

    key = translation_cache_key(raw)

    cache["entries"][key] = {

        "source_text": raw,
        "translated_text": translated,

        "created_at_utc": iso_utc(),

        "translation_version": (
            TRANSLATION_VERSION
        ),

    }


def cached_translation(cache, text):

    key = translation_cache_key(text)

    entry = cache["entries"].get(key)

    if not entry:

        return None

    return entry.get(
        "translated_text"
    )


# ============================================================
# MONTHLY API USAGE
# ============================================================

def current_month():

    return utc_now().strftime("%Y-%m")


def load_usage():

    usage = read_json(

        USAGE_PATH,

        {}

    )

    month = current_month()

    stored_month = (

        usage.get("month")

        or usage.get("month_utc")

    )

    if stored_month != month:

        return {

            "month": month,

            "characters_used": 0,

            "successful_requests": 0,

            "rejected_requests": 0,

            "rejected_characters": 0,

            "uncertain_requests": 0,

        }

    existing_characters = usage.get(

        "characters_used",

        usage.get(

            "characters",

            usage.get("used_characters", 0)

        )

    )

    return {

        "month": month,

        "characters_used": int(
            existing_characters
        ),

        "successful_requests": int(
            usage.get("successful_requests", 0)
        ),

        "rejected_requests": int(
            usage.get("rejected_requests", 0)
        ),

        "rejected_characters": int(
            usage.get("rejected_characters", 0)
        ),

        "uncertain_requests": int(
            usage.get("uncertain_requests", 0)
        ),

    }


def reserve_usage(usage, characters):

    if (

        usage["characters_used"] + characters

        > MONTHLY_CHARACTER_LIMIT

    ):

        raise TranslationBudgetExceeded(

            "Monthly translation safety limit reached"

        )

    usage["characters_used"] += characters

    usage["updated_at_utc"] = iso_utc()

    write_json(
        USAGE_PATH,
        usage
    )


def refund_rejected_usage(
    usage,
    characters
):

    usage["characters_used"] = max(

        0,

        usage["characters_used"]
        - characters

    )

    usage["rejected_requests"] += 1

    usage["rejected_characters"] += (
        characters
    )

    usage["updated_at_utc"] = iso_utc()

    write_json(
        USAGE_PATH,
        usage
    )


# ============================================================
# API REQUEST PACING
# ============================================================

def pace_request(characters):

    if characters > AZURE_CHARACTERS_PER_MINUTE:

        raise ValueError(

            "Request exceeds configured pacing limit"

        )

    while True:

        now = time.monotonic()

        while (

            REQUEST_HISTORY

            and now - REQUEST_HISTORY[0][0]
            >= 60

        ):

            REQUEST_HISTORY.popleft()

        recent_characters = sum(

            item[1]

            for item in REQUEST_HISTORY

        )

        if (

            recent_characters + characters

            <= AZURE_CHARACTERS_PER_MINUTE

        ):

            REQUEST_HISTORY.append((

                now,

                characters

            ))

            return

        wait = (

            60

            - (now - REQUEST_HISTORY[0][0])

            + 1

        )

        wait = max(1, wait)

        print(

            f"[AZURE] Pacing request: "

            f"waiting {wait:.1f}s"

        )

        time.sleep(wait)


# ============================================================
# AZURE TRANSLATOR
# ============================================================

def azure_translate(texts, usage):

    if not texts:

        return []

    if not AZURE_TRANSLATOR_KEY:

        raise TranslationUnavailable(

            "AZURE_TRANSLATOR_KEY is missing"

        )

    if not AZURE_TRANSLATOR_REGION:

        raise TranslationUnavailable(

            "AZURE_TRANSLATOR_REGION is missing"

        )

    characters = sum(

        len(text)

        for text in texts

    )

    if len(texts) > AZURE_MAX_REQUEST_ITEMS:

        raise ValueError(

            "Too many translation items"

        )

    headers = {

        "Ocp-Apim-Subscription-Key": (
            AZURE_TRANSLATOR_KEY
        ),

        "Ocp-Apim-Subscription-Region": (
            AZURE_TRANSLATOR_REGION
        ),

        "Content-Type": "application/json",

    }

    params = {

        "api-version": "3.0",
        "from": "cs",
        "to": "en",

    }

    body = [

        {
            "Text": text
        }

        for text in texts

    ]

    for attempt in range(
        MAX_TRANSLATION_ATTEMPTS
    ):

        pace_request(characters)

        reserve_usage(
            usage,
            characters
        )

        started = time.monotonic()

        try:

            response = SESSION.post(

                AZURE_ENDPOINT,

                params=params,

                headers=headers,

                json=body,

                timeout=AZURE_REQUEST_TIMEOUT

            )

        except requests.RequestException:

            usage["uncertain_requests"] += 1

            write_json(
                USAGE_PATH,
                usage
            )

            if (
                attempt + 1
                >= MAX_TRANSLATION_ATTEMPTS
            ):

                raise TranslationUnavailable(

                    "Azure network request failed"

                )

            delay = 2 ** (attempt + 1)

            print(

                f"[AZURE] Network error. "

                f"Retrying in {delay}s"

            )

            time.sleep(delay)

            continue

        elapsed = (

            time.monotonic() - started

        )

        print(

            f"[AZURE] HTTP "

            f"{response.status_code} | "

            f"{len(texts)} texts | "

            f"{characters:,} characters | "

            f"{elapsed:.2f}s"

        )

        if response.status_code == 200:

            usage["successful_requests"] += 1

            write_json(
                USAGE_PATH,
                usage
            )

            try:

                payload = response.json()

                translated = [

                    clean_text(

                        item["translations"][0]["text"]

                    )

                    for item in payload

                ]

            except Exception:

                raise TranslationUnavailable(

                    "Invalid Azure translation response"

                )

            if len(translated) != len(texts):

                raise TranslationUnavailable(

                    "Translation response length mismatch"

                )

            return translated

        if response.status_code == 429:

            refund_rejected_usage(

                usage,

                characters

            )

            if (

                attempt + 1

                >= MAX_TRANSLATION_ATTEMPTS

            ):

                break

            try:

                retry_after = float(

                    response.headers.get(

                        "Retry-After",

                        "65"

                    )

                )

            except ValueError:

                retry_after = 65

            delay = max(65, retry_after)

            print(

                f"[AZURE] Rate limited. "

                f"Retrying in {delay:.0f}s"

            )

            time.sleep(

                min(delay, 120)

            )

            continue

        if response.status_code in {

            400,

            401,

            403,

            404,

            413

        }:

            refund_rejected_usage(

                usage,

                characters

            )

            raise TranslationUnavailable(

                f"Azure rejected request: "

                f"HTTP {response.status_code}"

            )

        if response.status_code in {

            500,

            502,

            503,

            504

        }:

            usage["uncertain_requests"] += 1

            write_json(
                USAGE_PATH,
                usage
            )

            if (

                attempt + 1

                >= MAX_TRANSLATION_ATTEMPTS

            ):

                break

            delay = 2 ** (attempt + 1)

            print(

                f"[AZURE] Temporary server error. "

                f"Retrying in {delay}s"

            )

            time.sleep(delay)

            continue

        raise TranslationUnavailable(

            f"Unexpected Azure response: "

            f"HTTP {response.status_code}"

        )

    raise TranslationUnavailable(

        "Azure retry limit reached"

    )


# ============================================================
# SOURCE-AWARE TERMINOLOGY CORRECTIONS
# ============================================================

def correct_terminology(original, translated):

    if not translated:

        return translated

    source = original.lower()

    result = translated

    # Domestic Czech MPs are not European MEPs.
    if (

        re.search(

            r"\bposlanc\w*|\bposlankyn\w*",

            source

        )

        and not re.search(

            r"\beuroposlanc\w*|"

            r"\bevropsk\w*\s+parlament\w*",

            source

        )

    ):

        result = re.sub(

            r"\bMEPs\b",

            "MPs",

            result

        )

        result = re.sub(

            r"\bMEP\b",

            "MP",

            result

        )

        result = re.sub(

            r"\bMembers of the European Parliament\b",

            "Members of Parliament",

            result,

            flags=re.IGNORECASE

        )

    # Czech central bank.
    if re.search(

        r"\bČNB\b",

        original

    ):

        result = re.sub(

            r"\bCNB\b",

            "Czech National Bank",

            result

        )

    # Czech News Agency.
    if re.search(

        r"\bČTK\b",

        original

    ):

        result = re.sub(

            r"\bČTK\b|\bCTK\b",

            "Czech News Agency",

            result

        )

    # Disguised employment.
    if "švarcsystém" in source:

        result = re.sub(

            r"\bSchwarz system\b|"

            r"\bSvarc system\b|"

            r"\bŠvarc system\b",

            "disguised employment system",

            result,

            flags=re.IGNORECASE

        )

    # Court enforcement, not physical execution.
    if (

        re.search(

            r"\bexekuc\w*",

            source

        )

        and re.search(

            r"\bsoud\w*|\bomluv\w*|"

            r"\bdluh\w*|\bvymáh\w*|"

            r"\bvymoci\b",

            source

        )

    ):

        result = re.sub(

            r"\bthrough executions?\b",

            "through court enforcement proceedings",

            result,

            flags=re.IGNORECASE

        )

        result = re.sub(

            r"\bby executions?\b",

            "through court enforcement proceedings",

            result,

            flags=re.IGNORECASE

        )

    # District heating.
    if re.search(

        r"\bteplárn\w*",

        source

    ):

        result = re.sub(

            r"(?<!district )\bheating plants\b",

            "district heating plants",

            result,

            flags=re.IGNORECASE

        )

        result = re.sub(

            r"(?<!district )\bheating plant\b",

            "district heating plant",

            result,

            flags=re.IGNORECASE

        )

    return clean_text(result)


# ============================================================
# NUMERICAL NORMALIZATION
# ============================================================

def extract_numbers(text, language):

    text = unicodedata.normalize(

        "NFKC",

        text or ""

    ).lower()

    text = text.replace("\u00a0", " ")
    text = text.replace("\u202f", " ")
    text = text.replace("\u2009", " ")

    values = []

    # English 12-hour time expressions.
    if language == "en":

        def english_time(match):

            hour = int(match.group(1))
            minute = int(match.group(2) or 0)
            period = match.group(3)

            hour = hour % 12

            if period == "p":

                hour += 12

            return (

                f" {hour:02d}:{minute:02d} "

            )

        text = re.sub(

            r"\b(1[0-2]|[1-9])"

            r"(?::([0-5]\d))?"

            r"\s*([ap])\.?\s*m\.?(?!\w)",

            english_time,

            text

        )

    # Czech clock expressions.
    if language == "cs":

        text = re.sub(

            r"\b(?:v|od|do|po|před|kolem)"

            r"\s+([01]?\d|2[0-3])"

            r"\s+hodin(?:ě|ách|y)?\b",

            lambda match: (

                f" {int(match.group(1)):02d}:00 "

            ),

            text

        )

    # Clock times.
    time_pattern = (

        r"(?<!\d)([01]?\d|2[0-3]):"

        r"([0-5]\d)(?!\d)"

    )

    for match in re.finditer(

        time_pattern,

        text

    ):

        hour = int(match.group(1))

        minute = int(match.group(2))

        values.append((

            "time",

            hour * 60 + minute

        ))

    text = re.sub(

        time_pattern,

        " ",

        text

    )

    # Numeric calendar quarters.
    quarter_patterns = {

        "cs": [

            r"\b([1-4])\.\s*čtvrtlet\w*",

        ],

        "en": [

            r"\bq([1-4])\b",

            r"\b([1-4])(?:st|nd|rd|th)\s+quarter\b",

        ],

    }

    for pattern in quarter_patterns.get(

        language,

        []

    ):

        for match in re.finditer(

            pattern,

            text

        ):

            values.append((

                "quarter",

                int(match.group(1))

            ))

        text = re.sub(

            pattern,

            " ",

            text

        )

    # Written calendar quarters.
    quarter_words = {

        "cs": {

            "první": 1,
            "prvním": 1,

            "druhé": 2,
            "druhém": 2,

            "třetí": 3,
            "třetím": 3,

            "čtvrté": 4,
            "čtvrtém": 4,

        },

        "en": {

            "first": 1,
            "second": 2,
            "third": 3,
            "fourth": 4,

        },

    }

    for word, quarter in quarter_words.get(

        language,

        {}

    ).items():

        suffix = (

            r"čtvrtlet\w*"

            if language == "cs"

            else r"quarter\b"

        )

        pattern = (

            rf"\b{word}\s+{suffix}"

        )

        matches = list(

            re.finditer(

                pattern,

                text

            )

        )

        for _ in matches:

            values.append((

                "quarter",

                quarter

            ))

        text = re.sub(

            pattern,

            " ",

            text

        )

    # Normalize grouped thousands.
    if language == "cs":

        text = re.sub(

            r"\b\d{1,3}(?:[ .]\d{3})+"

            r"(?:,\d+)?\b",

            lambda match: (

                match.group(0)

                .replace(" ", "")

                .replace(".", "")

            ),

            text

        )

    else:

        text = re.sub(

            r"\b\d{1,3}(?:[, ]\d{3})+"

            r"(?:\.\d+)?\b",

            lambda match: (

                match.group(0)

                .replace(",", "")

                .replace(" ", "")

            ),

            text

        )

    magnitude_patterns = {

        "cs": {

            r"tisíc\w*|tis\.?":

                Decimal("1000"),

            r"milion\w*|milión\w*|mil\.?":

                Decimal("1000000"),

            r"miliard\w*|mld\.?":

                Decimal("1000000000"),

            r"bilion\w*|bilión\w*":

                Decimal("1000000000000"),

        },

        "en": {

            r"thousands?":

                Decimal("1000"),

            r"millions?":

                Decimal("1000000"),

            r"billions?":

                Decimal("1000000000"),

            r"trillions?":

                Decimal("1000000000000"),

        },

    }

    magnitudes = magnitude_patterns.get(

        language,

        {}

    )

    magnitude_expression = "|".join(

        f"(?:{pattern})"

        for pattern in magnitudes

    )

    number_pattern = re.compile(

        rf"(?<!\w)"

        rf"(\d+(?:[.,]\d+)?)"

        rf"(?:\s*({magnitude_expression}))?"

        rf"(?!\w)"

    )

    for match in number_pattern.finditer(

        text

    ):

        raw_number = match.group(1)

        magnitude = match.group(2)

        if language == "cs":

            raw_number = raw_number.replace(

                ",",

                "."

            )

        try:

            number = Decimal(raw_number)

        except InvalidOperation:

            continue

        if magnitude:

            for pattern, multiplier in (
                magnitudes.items()
            ):

                if re.fullmatch(

                    pattern,

                    magnitude

                ):

                    number *= multiplier

                    break

        values.append((

            "number",

            number.normalize()

        ))

    return Counter(values)


# ============================================================
# TRANSLATION VALIDATION
# ============================================================

PROTECTED_NAMES = {

    "Babiš": r"\bBabiš\b|\bBabis\b",

    "Okamura": r"\bOkamura\b",

    "Plaga": r"\bPlaga\b",

    "Agrofert": r"\bAgrofert\b",

    "Dukovany": r"\bDukovany\b",

    "Temelín": r"\bTemelín\b|\bTemelin\b",

}


def validate_translation(original, translated):

    warnings = []

    if not original:

        return warnings

    if not translated:

        return ["empty_translation"]

    source = original.lower()

    target = translated.lower()

    if len(original) >= 40:

        ratio = (

            len(translated)

            / max(1, len(original))

        )

        if ratio < 0.40:

            warnings.append(

                "translation_too_short"

            )

        if ratio > 3.0:

            warnings.append(

                "translation_too_long"

            )

    original_numbers = extract_numbers(

        original,

        "cs"

    )

    translated_numbers = extract_numbers(

        translated,

        "en"

    )

    missing = (

        original_numbers

        - translated_numbers

    )

    unexpected = (

        translated_numbers

        - original_numbers

    )

    if missing:

        warnings.append(

            "missing_numeric_values"

        )

    if unexpected:

        warnings.append(

            "unexpected_numeric_values"

        )

    # Preserve percentage references.
    original_percentages = bool(

        re.search(

            r"%|\bprocent\w*",

            source

        )

    )

    translated_percentages = bool(

        re.search(

            r"%|\bpercent\b|\bper cent\b",

            target

        )

    )

    if (

        original_percentages

        and not translated_percentages

    ):

        warnings.append(

            "missing_percentage_reference"

        )

    # Detect obvious currency substitutions.
    if (

        re.search(

            r"\bkorun\w*|\bKč\b",

            original,

            re.IGNORECASE

        )

        and re.search(

            r"\bSEK\b|\bSwedish kronor\b",

            translated,

            re.IGNORECASE

        )

    ):

        warnings.append(

            "incorrect_currency"

        )

    # Domestic versus European parliamentary terminology.
    if (

        re.search(

            r"\bposlanc\w*|\bposlankyn\w*",

            source

        )

        and not re.search(

            r"\beuroposlanc\w*|"

            r"\bevropsk\w*\s+parlament\w*",

            source

        )

        and re.search(

            r"\bMEPs?\b|"

            r"\bMembers of the European Parliament\b",

            translated,

            re.IGNORECASE

        )

    ):

        warnings.append(

            "incorrect_parliamentary_terminology"

        )

    # Proper-noun preservation.
    for name, pattern in (

        PROTECTED_NAMES.items()

    ):

        stem = name.lower()

        if stem.endswith("a"):

            stem = stem[:-1]

        if re.search(

            rf"\b{re.escape(stem)}\w*",

            source

        ):

            if not re.search(

                pattern,

                translated,

                re.IGNORECASE

            ):

                warnings.append(

                    f"missing_proper_name:{name}"

                )

    # Ukraine should not disappear.
    if re.search(

        r"\bukrajin\w*",

        source

    ):

        if not re.search(

            r"\bukrain\w*",

            target

        ):

            warnings.append(

                "missing_ukraine_reference"

            )

    # Known NLLB-style hallucination safeguard.
    if (

        "lisbon" in target

        and not re.search(

            r"\blisabon\w*",

            source

        )

    ):

        warnings.append(

            "unexpected_lisbon_reference"

        )

    # Check missile interception terminology.
    if re.search(

        r"\bsestřel\w*|\bzachyt\w*.{0,20}\braket\w*",

        source

    ):

        if re.search(

            r"\bmissile strikes?\b",

            target

        ):

            if not re.search(

                r"\bintercept\w*|\bshot down\b",

                target

            ):

                warnings.append(

                    "possible_interception_mistranslation"

                )

    return list(

        dict.fromkeys(warnings)

    )


# ============================================================
# TRANSLATION BATCHING
# ============================================================

def translate_texts(texts, cache, usage):

    translations = {}

    pending = []

    seen = set()

    for text in texts:

        if not text:

            translations[text] = ""

            continue

        if text in seen:

            continue

        seen.add(text)

        existing = cached_translation(

            cache,

            text

        )

        if existing is not None:

            translations[text] = existing

        else:

            pending.append(text)

    batch = []

    characters = 0

    request_limit = min(

        AZURE_MAX_REQUEST_CHARACTERS,

        AZURE_CHARACTERS_PER_MINUTE

    )

    def process_batch(items):

        if not items:

            return

        results = azure_translate(

            items,

            usage

        )

        for raw, translated in zip(

            items,

            results

        ):

            corrected = correct_terminology(

                raw,

                translated

            )

            translations[raw] = corrected

            # Cache successful API responses even when
            # validation later flags the article.
            cache_translation(

                cache,

                raw,

                corrected

            )

        write_json(

            CACHE_PATH,

            cache

        )

    for text in pending:

        if len(text) > request_limit:

            raise TranslationUnavailable(

                "A translation item is too large"

            )

        exceeds_count = (

            len(batch)

            >= AZURE_MAX_REQUEST_ITEMS

        )

        exceeds_characters = (

            characters + len(text)

            > request_limit

        )

        if batch and (

            exceeds_count

            or exceeds_characters

        ):

            process_batch(batch)

            batch = []

            characters = 0

        batch.append(text)

        characters += len(text)

    if batch:

        process_batch(batch)

    return translations


def needs_translation(article):

    if not article.get("relevant"):

        return False

    if article.get("source_language") == "en":

        return False

    status = article.get(

        "translation_status"

    )

    if (

        article.get("translation_version")

        == TRANSLATION_VERSION

        and status in {

            "completed",

            "review_required"

        }

    ):

        return False

    if (

        article.get("translation_attempts", 0)

        >= MAX_TRANSLATION_ATTEMPTS

    ):

        return False

    return True


def process_translation_batch(

    articles,

    cache,

    usage

):

    texts = []

    for article in articles:

        texts.append(

            article.get("title_raw", "")

        )

        texts.append(

            article.get("lead_raw", "")

        )

    translations = translate_texts(

        texts,

        cache,

        usage

    )

    for article in articles:

        title_raw = article.get(

            "title_raw",

            ""

        )

        lead_raw = article.get(

            "lead_raw",

            ""

        )

        title_en = translations.get(

            title_raw,

            ""

        )

        lead_en = translations.get(

            lead_raw,

            ""

        )

        title_warnings = validate_translation(

            title_raw,

            title_en

        )

        lead_warnings = validate_translation(

            lead_raw,

            lead_en

        )

        warnings = [

            f"title:{warning}"

            for warning in title_warnings

        ] + [

            f"lead:{warning}"

            for warning in lead_warnings

        ]

        article["title_en"] = title_en

        article["lead_en"] = lead_en

        article["translation_version"] = (

            TRANSLATION_VERSION

        )

        article["validation_version"] = (

            VALIDATION_VERSION

        )

        article["translation_attempts"] = (

            article.get(

                "translation_attempts",

                0

            ) + 1

        )

        article["translation_provider"] = (

            "Microsoft Azure Translator"

        )

        article["translation_method"] = (

            "whole_headline_and_lead"

        )

        article["translation_error"] = None

        article["translation_warnings"] = (

            warnings

        )

        article["translated_at_utc"] = (

            iso_utc()

        )

        if warnings:

            article["translation_status"] = (

                "review_required"

            )

            print(

                f"[REVIEW] {title_raw}"

            )

            print(

                "[REVIEW] "

                + ", ".join(warnings)

            )

        else:

            article["translation_status"] = (

                "completed"

            )

    return len(articles)


# ============================================================
# RECLASSIFICATION
# ============================================================

def update_classifications(archive):

    updated = 0

    for article in archive.values():

        if (

            article.get("classification_version")

            == CLASSIFICATION_VERSION

        ):

            continue

        article.update(

            classify_topics(article)

        )

        updated += 1

        if article.get("source_language") == "en":

            article["translation_status"] = (

                "not_required"

            )

            article["title_en"] = article.get(

                "title_raw",

                ""

            )

            article["lead_en"] = article.get(

                "lead_raw",

                ""

            )

        elif not article.get("relevant"):

            article["translation_status"] = (

                "not_relevant"

            )

        elif article.get(

            "translation_status"

        ) == "not_relevant":

            article["translation_status"] = (

                "pending"

            )

    if updated:

        print(

            f"[CLASSIFICATION] "

            f"Reclassified {updated} articles"

        )


# ============================================================
# REVALIDATE EXISTING TRANSLATIONS
# ============================================================

def revalidate_existing(archive, cache):

    updated = 0

    for article in archive.values():

        if article.get("source_language") != "cs":

            continue

        if not article.get("relevant"):

            continue

        if not article.get("title_en"):

            continue

        if (

            article.get("validation_version")

            == VALIDATION_VERSION

            and article.get("translation_version")

            == TRANSLATION_VERSION

        ):

            continue

        title_raw = article.get(

            "title_raw",

            ""

        )

        lead_raw = article.get(

            "lead_raw",

            ""

        )

        title_en = correct_terminology(

            title_raw,

            article.get("title_en", "")

        )

        lead_en = correct_terminology(

            lead_raw,

            article.get("lead_en") or ""

        )

        warnings = [

            f"title:{warning}"

            for warning in validate_translation(

                title_raw,

                title_en

            )

        ] + [

            f"lead:{warning}"

            for warning in validate_translation(

                lead_raw,

                lead_en

            )

        ]

        article["title_en"] = title_en

        article["lead_en"] = lead_en

        article["translation_version"] = (

            TRANSLATION_VERSION

        )

        article["validation_version"] = (

            VALIDATION_VERSION

        )

        article["translation_warnings"] = (

            warnings

        )

        article["translation_status"] = (

            "review_required"

            if warnings

            else "completed"

        )

        article["translation_error"] = None

        cache_translation(

            cache,

            title_raw,

            title_en

        )

        if lead_raw:

            cache_translation(

                cache,

                lead_raw,

                lead_en

            )

        updated += 1

    if updated:

        print(

            f"[VALIDATION] Revalidated "

            f"{updated} existing translations "

            f"without Azure requests"

        )


# ============================================================
# JSON OUTPUTS
# ============================================================

def publishable(article):

    return bool(

        article.get("relevant")

        and article.get("title_en")

        and article.get("translation_status") in {

            "completed",

            "not_required"

        }

    )


def build_payload(articles, topic=None):

    payload = {

        "country": "Czech Republic",

        "country_code": "CZ",

        "updated_at_utc": iso_utc(),

        "count": len(articles),

        "articles": articles,

    }

    if topic:

        payload["topic"] = topic

    return payload


def write_output_files(

    archive,

    cache,

    usage

):

    active = get_active_articles(

        archive

    )

    published = [

        article

        for article in active

        if publishable(article)

    ]

    write_json(

        DATA_DIR / "latest.json",

        build_payload(published)

    )

    print(

        f"[OUTPUT] Published articles: "

        f"{len(published)}"

    )

    for topic in TOPICS:

        articles = [

            article

            for article in published

            if topic in article.get(

                "topics",

                []

            )

        ]

        write_json(

            DATA_DIR / f"{topic}.json",

            build_payload(

                articles,

                topic

            )

        )

        print(

            f"[OUTPUT] {topic}: "

            f"{len(articles)}"

        )

    review = [

        article

        for article in active

        if article.get("relevant")

        and article.get("translation_status")

        == "review_required"

    ]

    pending = [

        article

        for article in active

        if article.get("relevant")

        and article.get("translation_status")

        in {

            "pending",

            "failed"

        }

    ]

    write_json(

        DATA_DIR / "review_required.json",

        build_payload(review)

    )

    write_json(

        DATA_DIR / "pending.json",

        build_payload(pending)

    )

    all_articles = sorted(

        archive.values(),

        key=lambda article: article.get(

            "published_utc",

            ""

        ),

        reverse=True

    )

    write_json(

        ARCHIVE_PATH,

        build_payload(all_articles)

    )

    write_json(

        CACHE_PATH,

        cache

    )

    write_json(

        USAGE_PATH,

        usage

    )

    print(

        f"[OUTPUT] Review required: "

        f"{len(review)}"

    )

    print(

        f"[OUTPUT] Pending: "

        f"{len(pending)}"

    )


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():

    started = time.monotonic()

    DATA_DIR.mkdir(

        parents=True,

        exist_ok=True

    )

    print("=" * 70)

    print(

        f"CZECH MEDIA COLLECTOR "

        f"V{COLLECTOR_VERSION}"

    )

    print("Microsoft Azure Translator")

    print("=" * 70)

    print(

        f"[CONFIG] Public retention: "

        f"{KEEP_HOURS} hours"

    )

    print(

        f"[CONFIG] Archive retention: "

        f"{ARCHIVE_RETENTION_DAYS} days"

    )

    print(

        f"[CONFIG] Cache retention: "

        f"{CACHE_RETENTION_DAYS} days"

    )

    print(

        f"[CONFIG] Azure region: "

        f"{AZURE_TRANSLATOR_REGION}"

    )

    print(

        f"[CONFIG] Monthly safety limit: "

        f"{MONTHLY_CHARACTER_LIMIT:,} characters"

    )

    print(

        f"[CONFIG] API pacing limit: "

        f"{AZURE_CHARACTERS_PER_MINUTE:,} "

        f"characters per minute"

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

        f"{len(cache['entries'])}"

    )

    print(

        f"[USAGE] Characters used this month: "

        f"{usage['characters_used']:,}"

    )

    collect_articles(archive)

    prune_archive(archive)

    update_classifications(archive)

    revalidate_existing(

        archive,

        cache

    )

    active = get_active_articles(

        archive

    )

    queue = [

        article

        for article in active

        if needs_translation(article)

    ]

    print(

        f"[QUEUE] Articles requiring translation: "

        f"{len(queue)}"

    )

    processed = 0

    for index in range(

        0,

        len(queue),

        TRANSLATION_BATCH_SIZE

    ):

        batch = queue[

            index:

            index + TRANSLATION_BATCH_SIZE

        ]

        try:

            processed += process_translation_batch(

                batch,

                cache,

                usage

            )

        except TranslationBudgetExceeded as exc:

            print(

                f"[WARN] {exc}"

            )

            break

        except TranslationUnavailable as exc:

            print(

                f"[WARN] Translation unavailable: "

                f"{exc}"

            )

            break

    write_output_files(

        archive,

        cache,

        usage

    )

    active = get_active_articles(

        archive

    )

    statuses = Counter(

        article.get(

            "translation_status",

            "unknown"

        )

        for article in active

    )

    relevant = sum(

        1

        for article in active

        if article.get("relevant")

    )

    elapsed = (

        time.monotonic()

        - started

    )

    print("=" * 70)

    print("FINAL STATISTICS")

    print("=" * 70)

    print(

        f"[STATS] Archive articles: "

        f"{len(archive)}"

    )

    print(

        f"[STATS] Active articles: "

        f"{len(active)}"

    )

    print(

        f"[STATS] Relevant active articles: "

        f"{relevant}"

    )

    print(

        f"[STATS] Articles processed: "

        f"{processed}"

    )

    for status, count in statuses.items():

        print(

            f"[STATUS] {status}: "

            f"{count}"

        )

    print(

        f"[USAGE] Monthly characters: "

        f"{usage['characters_used']:,} / "

        f"{MONTHLY_CHARACTER_LIMIT:,}"

    )

    print(

        f"[USAGE] Successful requests: "

        f"{usage['successful_requests']}"

    )

    print(

        f"[USAGE] Rejected requests: "

        f"{usage['rejected_requests']}"

    )

    print(

        f"[USAGE] Rejected characters: "

        f"{usage['rejected_characters']:,}"

    )

    print(

        f"[CACHE] Stored translations: "

        f"{len(cache['entries'])}"

    )

    print(

        f"[STATS] Runtime: "

        f"{elapsed:.2f} seconds"

    )

    print("=" * 70)


if __name__ == "__main__":

    main()
