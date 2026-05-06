from types import SimpleNamespace

from forex_shared.domain.intel import IntelItem

from collector_events.nlp.session import NLPEnrichmentSession
from collector_events.processors.event_processor import EventProcessor
from collector_events.translation.session import TranslationSession


class FakeTranslationEngine:
    translation_provider = "fake"
    target_language = "eng_Latn"

    def load(self):
        return None

    def detect_language(self, text):
        return SimpleNamespace(
            language="eng_Latn",
            language_name="English",
            confidence=1.0,
            status="detected",
            error="",
        )

    def translate(self, text, source_language):
        return text, "skipped", ""


class FakeEventProcessor:
    def process_item(self, item):
        item.extra["danger_score"] = 0.82
        item.extra["impact_category"] = "conflict"
        item.extra["nlp_features"] = {"sentiment": "negative"}
        item.extra["domain_weight"] = 1.2
        item.extra["source_signal_adjustment"] = 0.07
        item.extra["cluster_signal_adjustment"] = 0.11
        item.extra["attention_score"] = 0.91
        return SimpleNamespace(danger_score=0.82)


def _telegram_payload():
    return {
        "id": "telegram:item:1",
        "source": "telegram",
        "domain": "conflict",
        "title": "Border escalation reported",
        "body": "Multiple channels report cross-border fire near a strategic crossing.",
        "published_at": "2026-05-06T13:49:12+00:00",
        "tags": ["telegram", "osint"],
        "extra": {
            "source_score": 0.91,
            "source_role": "primary",
            "source_authenticity_class": "independent_reporter",
            "source_bias_risk": "medium",
            "verification_required": False,
            "cluster_id": "cluster:conflict:test",
            "cluster_emit_count": 3,
            "cluster_channel_count": 2,
            "cluster_weighted_attention": 1.82,
            "cluster_velocity_per_hour": 4.5,
        },
    }


def test_translation_preserves_source_and_cluster_metadata():
    session = TranslationSession(
        {"session_id": "test", "min_detection_confidence": 0.35},
        engine=FakeTranslationEngine(),
    )

    translated = session._process_payload(_telegram_payload())

    assert translated["event_type"] == "INTEL_ITEM_TRANSLATED"
    assert translated["extra"]["source_score"] == 0.91
    assert translated["extra"]["cluster_id"] == "cluster:conflict:test"
    assert translated["extra"]["cluster_channel_count"] == 2
    assert "No translation needed" in translated["extra"]["translation_source"]


def test_nlp_enrichment_preserves_metadata_and_publishes_enriched_contract():
    session = NLPEnrichmentSession(
        {"session_id": "test", "oracle_threshold": 0.68},
        processor=FakeEventProcessor(),
    )

    enriched = session._process_payload(_telegram_payload())

    assert enriched["event_type"] == "INTEL_ITEM_ENRICHED"
    assert enriched["extra"]["source_score"] == 0.91
    assert enriched["extra"]["cluster_id"] == "cluster:conflict:test"
    assert enriched["extra"]["nlp_processed"] is True
    assert enriched["extra"]["danger_score"] == 0.82
    assert enriched["extra"]["oracle_review_candidate"] is True


def test_telegram_translation_to_nlp_e2e_payload_contract():
    translation_session = TranslationSession(
        {"session_id": "translation-test", "min_detection_confidence": 0.35},
        engine=FakeTranslationEngine(),
    )
    nlp_session = NLPEnrichmentSession(
        {"session_id": "nlp-test", "oracle_threshold": 0.68},
        processor=FakeEventProcessor(),
    )

    translated = translation_session._process_payload(_telegram_payload())
    enriched = nlp_session._process_payload(translated)
    extra = enriched["extra"]

    assert enriched["event_type"] == "INTEL_ITEM_ENRICHED"
    assert "translation_source" in extra
    assert extra["source_score"] == 0.91
    assert extra["cluster_id"] == "cluster:conflict:test"
    assert extra["cluster_emit_count"] == 3
    assert extra["cluster_channel_count"] == 2
    assert extra["cluster_weighted_attention"] == 1.82
    assert extra["cluster_velocity_per_hour"] == 4.5
    assert extra["danger_score"] == 0.82
    assert extra["attention_score"] == 0.91
    assert extra["oracle_review_candidate"] is True


def test_source_cluster_adjustments_are_backward_compatible_and_source_aware():
    processor = EventProcessor.__new__(EventProcessor)
    processor.source_signal_weights = {
        "source_score_scale": 0.18,
        "cluster_channel_count_weight": 0.03,
        "cluster_emit_count_weight": 0.012,
        "cluster_weighted_attention_weight": 0.035,
        "cluster_velocity_weight": 0.02,
        "verification_required_penalty": 0.08,
        "medium_bias_penalty": 0.03,
        "independent_reporter_bonus": 0.03,
    }

    plain = IntelItem(
        id="plain",
        source="rss",
        domain="market",
        title="Regular market update",
    )
    plain_source_adj, plain_cluster_adj, plain_attention = processor._source_cluster_adjustments(plain)

    telegram = IntelItem.from_dict(_telegram_payload())
    source_adj, cluster_adj, attention = processor._source_cluster_adjustments(telegram)

    assert plain_source_adj == 0.0
    assert plain_cluster_adj == 0.0
    assert plain_attention == 0.5
    assert source_adj > plain_source_adj
    assert cluster_adj > plain_cluster_adj
    assert attention > plain_attention
