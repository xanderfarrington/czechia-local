from collector import (
    split_sentences,
    classify_topics,
    protect_terms,
    restore_terms,
    validate_translation,
    needs_upgrade,
    utcnow,
    TRANSLATION_VERSION,
    CLASSIFICATION_VERSION,
)


def make_article(title, lead="", url=""):

    return {
        "title_raw": title,
        "lead_raw": lead,
        "source_language": "cs",
        "source": "Test",
        "url": url,
    }


# ============================================================
# SENTENCE SEGMENTATION
# ============================================================

def test_multi_sentence_headline():

    text = (
        "Mzdy mají růst i příští rok. "
        "Přidávat chce přes dvě třetiny firem."
    )

    assert len(split_sentences(text)) == 2


def test_abbreviation_protection():

    text = (
        "Cena vzrostla např. v Praze. "
        "Další zvýšení se očekává."
    )

    assert len(split_sentences(text)) == 2


def test_czech_dates():

    text = (
        "Jednání proběhne 25. listopadu. "
        "Výsledky budou zveřejněny později."
    )

    assert len(split_sentences(text)) == 2


def test_quoted_sentences():

    text = (
        "‚Prostě nechci.‘ "
        "Obyvatelé odmítli větrné elektrárny."
    )

    assert len(split_sentences(text)) == 2


# ============================================================
# CLASSIFICATION REGRESSION TESTS
# ============================================================

def test_nato_substring():

    article = make_article(
        "Reportér chtěl natočit video."
    )

    classify_topics(article)

    assert "military" not in article["topics"]


def test_actual_nato():

    article = make_article(
        "NATO posiluje obranu."
    )

    classify_topics(article)

    assert "military" in article["topics"]


def test_hybrid_vehicle_not_military():

    article = make_article(
        "EU zavádí cla na hybridní automobily."
    )

    classify_topics(article)

    assert "military" not in article["topics"]


def test_hybrid_warfare():

    article = make_article(
        "Evropa čelí hybridní válce."
    )

    classify_topics(article)

    assert "military" in article["topics"]


def test_employee_not_economy():

    article = make_article(
        "Ředitel plánoval cestu do Karibiku.",
        "Pozval dva své zaměstnance."
    )

    classify_topics(article)

    assert "economy" not in article["topics"]


def test_expert_not_trade_union():

    article = make_article(
        "Ředitel navštívil Karibik.",
        "Doprovázeli ho odborní pracovníci."
    )

    classify_topics(article)

    assert "economy" not in article["topics"]


def test_agricultural_subsidies():

    article = make_article(
        "Zemědělci obdrží 31 miliard korun "
        "z evropských dotací."
    )

    classify_topics(article)

    assert "economy" in article["topics"]


def test_refinery_attack():

    article = make_article(
        "Ukrajinci zaútočili na dvě rafinerie."
    )

    classify_topics(article)

    assert "military" in article["topics"]
    assert "energy" in article["topics"]


def test_foreign_domestic_politics():

    article = make_article(
        "Slovenský premiér vystoupil v parlamentu."
    )

    classify_topics(article)

    assert (
        "domestic_politics"
        not in article["topics"]
    )


def test_czech_domestic_politics():

    article = make_article(
        "Sněmovna schválila změny zákona."
    )

    classify_topics(article)

    assert (
        "domestic_politics"
        in article["topics"]
    )


def test_generic_energy_not_enough():

    article = make_article(
        "Meta představila nového AI pomocníka.",
        "Pomocník šetří uživatelům čas a energii."
    )

    classify_topics(article)

    assert "energy" not in article["topics"]


# ============================================================
# ENTITY PROTECTION
# ============================================================

def test_babis_protection():

    text = (
        "Babiš chce konzultovat návrh."
    )

    protected, replacements = protect_terms(
        text,
        [],
    )

    assert "Babiš" in replacements.values()

    assert "Babiš" not in protected


def test_unknown_entity_protection():

    text = (
        "Nováková představila nový návrh."
    )

    entities = [{
        "start": 0,
        "end": 8,
        "text": "Nováková",
        "canonical": "Nováková",
        "type": "PER",
    }]

    protected, replacements = protect_terms(
        text,
        entities,
    )

    assert "Nováková" in replacements.values()


def test_glossary_restoration():

    text, missing = restore_terms(
        "ZXQ0QXZ announced a proposal.",
        {"ZXQ0QXZ": "Babiš"},
    )

    assert text == (
        "Babiš announced a proposal."
    )

    assert not missing


def test_uohs_glossary():

    protected, replacements = protect_terms(
        "Babiš jednal s vedením ÚOHS.",
        [],
    )

    assert (
        "Office for the Protection of Competition"
        in replacements.values()
    )


# ============================================================
# VALIDATION REGRESSION TESTS
# ============================================================

def test_missing_number():

    warnings = validate_translation(
        "Cena vzrostla o 75 procent.",
        "The price increased.",
    )

    assert any(
        "missing_numbers" in warning
        for warning in warnings
    )


def test_currency_error():

    warnings = validate_translation(
        "Cena vzrostla na 100 Kč.",
        "The price rose to 100 cents.",
    )

    assert (
        "possible_currency_error"
        in warnings
    )


def test_wrong_institution():

    warnings = validate_translation(
        "Babiš chce změnit vedení ÚOHS.",
        "Babiš wants to change the leadership "
        "of the CIA.",
    )

    assert (
        "incorrect_institution"
        in warnings
    )


def test_missile_semantic_error():

    warnings = validate_translation(
        "Sestřelování raket nad Ukrajinou.",
        "Missile strikes over Ukraine.",
    )

    assert (
        "incorrect_military_action"
        in warnings
    )


def test_dry_rot():

    warnings = validate_translation(
        "Napadla ji dřevomorka.",
        "She was attacked by a woodpecker.",
    )

    assert (
        "incorrect_technical_term"
        in warnings
    )


def test_fatalities_not_casualties():

    warnings = validate_translation(
        "Padlých Rusů může být půl milionu.",
        "Russian casualties may be half a million.",
    )

    assert (
        "fatalities_translated_as_casualties"
        in warnings
    )


def test_valid_percentage():

    warnings = validate_translation(
        "Cena vzrostla o 3,5 procenta.",
        "The price increased by 3.5 percent.",
    )

    assert not warnings


# ============================================================
# VERSION MIGRATION
# ============================================================

def test_current_translation_not_upgraded():

    article = {
        "source_language": "cs",
        "translation_status": "completed",
        "translation_version": TRANSLATION_VERSION,
        "relevant": True,
        "published_utc": utcnow().isoformat(),
    }

    assert not needs_upgrade(
        article,
        utcnow(),
    )


def test_legacy_translation_upgraded():

    article = {
        "source_language": "cs",
        "translation_status": "completed",
        "translation_version": 2,
        "relevant": True,
        "published_utc": utcnow().isoformat(),
    }

    assert needs_upgrade(
        article,
        utcnow(),
    )
