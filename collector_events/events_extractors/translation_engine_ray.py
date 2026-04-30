"""Ray/MQ language detection and translation enrichment for OSINT events.

Pipeline:

    intel.events.#  ->  TranslationEngineRay  ->  intel.translated.<domain>

The engine keeps the Telegram relay and other collectors focused on collection.
It consumes normalized ``IntelItem`` payloads from MQ, sends each payload to a
Ray actor, detects the source language with GlotLID, translates non-English
content when a local translator is configured, and republishes an enriched event.

Run:

    python -m collector_events.events_extractors.translation_engine_ray

Useful dry-run:

    python -m collector_events.events_extractors.translation_engine_ray --dry-run-text "..."

Model defaults:

    GLOTLID_MODEL_PATH=.models/translation/glotlid/model.bin
    TRANSLATION_PROVIDER=none

Set ``TRANSLATION_PROVIDER=nllb`` to use NLLB-200 distilled 600M, or
``TRANSLATION_PROVIDER=opus_mt`` for Helsinki-NLP/opus-mt-mul-en. The default
provider is intentionally ``none`` so the service never downloads or loads a
large translation model unless explicitly enabled.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import logging
import os
import re
import signal
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ray
from dotenv import load_dotenv


THIS_FILE = Path(__file__).resolve()
EXTRACTOR_DIR = THIS_FILE.parent
PACKAGE_ROOT = THIS_FILE.parents[1]
SERVICE_ROOT = THIS_FILE.parents[2]
DEFAULT_MODELS_DIR = SERVICE_ROOT / ".models" / "translation"
DEFAULT_GLOTLID_MODEL = DEFAULT_MODELS_DIR / "glotlid" / "model.bin"

if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

try:
    from forex_shared.logging.get_logger import get_logger, setup_logging
    from forex_shared.providers.mq.rabbitmq_provider_async import RabbitMQProviderAsync
except ImportError:  # pragma: no cover - direct execution fallback
    get_logger = None  # type: ignore[assignment]
    setup_logging = None  # type: ignore[assignment]
    RabbitMQProviderAsync = None  # type: ignore[assignment]


if setup_logging is not None:
    setup_logging()

log = get_logger(__name__) if get_logger is not None else logging.getLogger(__name__)


DEFAULT_INPUT_TOPIC = "intel.events.#"
DEFAULT_OUTPUT_PREFIX = "intel.translated"
DEFAULT_HEALTH_TOPIC = "intel.translation.health"
DEFAULT_RAY_NAMESPACE = "osint_translation"
DEFAULT_TARGET_LANGUAGE = "eng_Latn"
DEFAULT_GLOTLID_REPO_ID = "cis-lmu/glotlid"
DEFAULT_NLLB_MODEL = "facebook/nllb-200-distilled-600M"
DEFAULT_OPUS_MT_MODEL = "Helsinki-NLP/opus-mt-mul-en"

ENGLISH_CODES = {"en", "eng", "eng_latn", "eng_Latn", "english"}

ISO2_TO_NLLB = {
    "ar": "arb_Arab",
    "de": "deu_Latn",
    "el": "ell_Grek",
    "en": "eng_Latn",
    "es": "spa_Latn",
    "fa": "pes_Arab",
    "fr": "fra_Latn",
    "he": "heb_Hebr",
    "hi": "hin_Deva",
    "it": "ita_Latn",
    "ja": "jpn_Jpan",
    "ko": "kor_Hang",
    "pl": "pol_Latn",
    "pt": "por_Latn",
    "ru": "rus_Cyrl",
    "tr": "tur_Latn",
    "uk": "ukr_Cyrl",
    "zh": "zho_Hans",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except ValueError:
        return default


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _compact_text(value: Any, *, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _normalize_label(label: str) -> str:
    raw = str(label or "").strip()
    raw = raw.removeprefix("__label__").replace("-", "_")
    if not raw:
        return "und"

    lower = raw.lower()
    if lower in ENGLISH_CODES:
        return DEFAULT_TARGET_LANGUAGE

    if lower in ISO2_TO_NLLB:
        return ISO2_TO_NLLB[lower]

    return raw


def _language_name(language_code: str) -> str:
    code = _normalize_label(language_code)
    if code == "und":
        return "Unknown"
    if code == DEFAULT_TARGET_LANGUAGE:
        return "English"

    alpha = code.split("_", 1)[0]
    try:
        import pycountry

        lang = pycountry.languages.get(alpha_3=alpha)
        if lang is None and len(alpha) == 2:
            lang = pycountry.languages.get(alpha_2=alpha)
        if lang is not None:
            return getattr(lang, "name", code)
    except Exception:
        pass
    return code


def _is_english(language_code: str) -> bool:
    normalized = _normalize_label(language_code)
    return normalized == DEFAULT_TARGET_LANGUAGE or normalized.lower() in ENGLISH_CODES


def _topic_for_payload(prefix: str, payload: dict[str, Any], fixed_topic: str = "") -> str:
    if fixed_topic.strip():
        return fixed_topic.strip()
    domain = re.sub(r"[^a-z0-9_]+", "_", str(payload.get("domain") or "social").lower()).strip("_")
    return f"{prefix.rstrip('.')}.{domain or 'social'}"


def _safe_extra(payload: dict[str, Any]) -> dict[str, Any]:
    extra = payload.get("extra")
    if isinstance(extra, dict):
        return extra
    extra = {}
    payload["extra"] = extra
    return extra


def _extract_text_fields(payload: dict[str, Any], *, max_chars: int) -> dict[str, str]:
    title = str(payload.get("title") or "").strip()
    body = str(payload.get("body") or "").strip()

    raw = payload.get("raw")
    raw_text = ""
    if isinstance(raw, dict):
        raw_text = str(raw.get("text") or raw.get("body") or raw.get("message") or "").strip()

    if not body and raw_text:
        body = raw_text
    if not title and body:
        title = _compact_text(body, limit=280)

    sample = "\n".join(part for part in (title, body) if part).strip()
    return {
        "title": title[:max_chars],
        "body": body[:max_chars],
        "sample": sample[:max_chars],
    }


def download_glotlid_model(model_path: Path, *, repo_id: str = DEFAULT_GLOTLID_REPO_ID) -> Path:
    """Download GlotLID's fastText model from Hugging Face."""

    model_path = model_path.expanduser().resolve()
    model_path.parent.mkdir(parents=True, exist_ok=True)
    if model_path.exists():
        return model_path

    try:
        from huggingface_hub import hf_hub_download
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required to download GlotLID. "
            "Install with: pip install huggingface-hub"
        ) from exc

    downloaded = hf_hub_download(
        repo_id=repo_id,
        filename="model.bin",
        local_dir=str(model_path.parent),
        local_dir_use_symlinks=False,
    )
    return Path(downloaded)


@dataclass(frozen=True)
class TranslationEngineConfig:
    input_topic: str = DEFAULT_INPUT_TOPIC
    output_prefix: str = DEFAULT_OUTPUT_PREFIX
    output_topic: str = ""
    health_topic: str = DEFAULT_HEALTH_TOPIC
    health_interval_seconds: float = 30.0

    num_actors: int = 2
    actor_max_concurrency: int = 4
    max_inflight_per_actor: int = 32
    ray_address: str = ""
    ray_namespace: str = DEFAULT_RAY_NAMESPACE

    detector_model_path: Path = DEFAULT_GLOTLID_MODEL
    detector_repo_id: str = DEFAULT_GLOTLID_REPO_ID
    detector_auto_download: bool = False
    min_detection_confidence: float = 0.35

    translation_provider: str = "none"
    translation_model_path: str = ""
    target_language: str = DEFAULT_TARGET_LANGUAGE
    max_text_chars: int = 4000
    device: str = "auto"
    nllb_num_beams: int = 2
    nllb_max_new_tokens: int = 512

    @classmethod
    def from_env(cls) -> "TranslationEngineConfig":
        provider = (
            os.getenv("TRANSLATION_PROVIDER")
            or os.getenv("TRANSLATION_MODEL_PROVIDER")
            or "none"
        ).strip().lower()

        return cls(
            input_topic=os.getenv("TRANSLATION_INPUT_TOPIC", DEFAULT_INPUT_TOPIC).strip()
            or DEFAULT_INPUT_TOPIC,
            output_prefix=os.getenv("TRANSLATION_OUTPUT_PREFIX", DEFAULT_OUTPUT_PREFIX).strip()
            or DEFAULT_OUTPUT_PREFIX,
            output_topic=os.getenv("TRANSLATION_OUTPUT_TOPIC", "").strip(),
            health_topic=os.getenv("TRANSLATION_HEALTH_TOPIC", DEFAULT_HEALTH_TOPIC).strip()
            or DEFAULT_HEALTH_TOPIC,
            health_interval_seconds=_float_env("TRANSLATION_HEALTH_INTERVAL_SECONDS", 30.0, minimum=5.0),
            num_actors=_int_env("TRANSLATION_RAY_ACTORS", 2, minimum=1),
            actor_max_concurrency=_int_env("TRANSLATION_RAY_ACTOR_CONCURRENCY", 4, minimum=1),
            max_inflight_per_actor=_int_env("TRANSLATION_RAY_INFLIGHT_PER_ACTOR", 32, minimum=1),
            ray_address=os.getenv("TRANSLATION_RAY_ADDRESS", os.getenv("RAY_ADDRESS", "")).strip(),
            ray_namespace=os.getenv("TRANSLATION_RAY_NAMESPACE", DEFAULT_RAY_NAMESPACE).strip()
            or DEFAULT_RAY_NAMESPACE,
            detector_model_path=Path(os.getenv("GLOTLID_MODEL_PATH", str(DEFAULT_GLOTLID_MODEL))),
            detector_repo_id=os.getenv("GLOTLID_REPO_ID", DEFAULT_GLOTLID_REPO_ID).strip()
            or DEFAULT_GLOTLID_REPO_ID,
            detector_auto_download=_bool_env("GLOTLID_AUTO_DOWNLOAD", False),
            min_detection_confidence=_float_env("TRANSLATION_MIN_DETECTION_CONFIDENCE", 0.35),
            translation_provider=provider,
            translation_model_path=os.getenv("TRANSLATION_MODEL_PATH", "").strip(),
            target_language=os.getenv("TRANSLATION_TARGET_LANGUAGE", DEFAULT_TARGET_LANGUAGE).strip()
            or DEFAULT_TARGET_LANGUAGE,
            max_text_chars=_int_env("TRANSLATION_MAX_TEXT_CHARS", 4000, minimum=200),
            device=os.getenv("TRANSLATION_DEVICE", "auto").strip() or "auto",
            nllb_num_beams=_int_env("TRANSLATION_NLLB_NUM_BEAMS", 2, minimum=1),
            nllb_max_new_tokens=_int_env("TRANSLATION_NLLB_MAX_NEW_TOKENS", 512, minimum=32),
        )


@dataclass(frozen=True)
class DetectionResult:
    language: str
    language_name: str
    confidence: float
    detector: str
    status: str
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@ray.remote(max_restarts=3, max_task_retries=1)
class TranslationWorkerActor:
    """Ray actor that owns one GlotLID detector and one optional translator."""

    def __init__(self, *, actor_id: str, config: dict[str, Any]) -> None:
        if setup_logging is not None:
            setup_logging()
        self._log = get_logger(f"{__name__}.actor.{actor_id}") if get_logger is not None else logging.getLogger(actor_id)

        self.actor_id = actor_id
        self.config = config
        self.detector_model_path = Path(config["detector_model_path"])
        self.detector_repo_id = str(config["detector_repo_id"])
        self.detector_auto_download = bool(config["detector_auto_download"])
        self.min_detection_confidence = float(config["min_detection_confidence"])
        self.translation_provider = str(config["translation_provider"]).lower().strip() or "none"
        self.translation_model_path = str(config["translation_model_path"]).strip()
        self.target_language = str(config["target_language"]).strip() or DEFAULT_TARGET_LANGUAGE
        self.max_text_chars = int(config["max_text_chars"])
        self.device = str(config["device"]).strip() or "auto"
        self.nllb_num_beams = int(config["nllb_num_beams"])
        self.nllb_max_new_tokens = int(config["nllb_max_new_tokens"])

        self._detector: Any = None
        self._tokenizer: Any = None
        self._translation_model: Any = None
        self._translation_pipeline: Any = None

        self.received = 0
        self.translated = 0
        self.detected_only = 0
        self.skipped = 0
        self.errors = 0
        self.last_error = ""
        self.started_at = _utc_now()

    def start(self) -> dict[str, Any]:
        self._load_detector()
        self._load_translator()
        return self.health()

    def stop(self) -> dict[str, Any]:
        self._detector = None
        self._tokenizer = None
        self._translation_model = None
        self._translation_pipeline = None
        return self.health()

    def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.received += 1
        try:
            if not isinstance(payload, dict):
                self.skipped += 1
                return {"ok": False, "reason": "payload is not a dict", "payload": {}}

            enriched = copy.deepcopy(payload)
            if str(enriched.get("event_type") or "").upper() == "INTEL_ITEM_TRANSLATED":
                self.skipped += 1
                return {"ok": True, "skipped": True, "payload": enriched}

            text_fields = _extract_text_fields(enriched, max_chars=self.max_text_chars)
            sample = text_fields["sample"]
            detection = self._detect_language(sample)

            original_event_type = enriched.get("event_type")
            should_translate = self._should_translate(detection, sample)
            translated_title = text_fields["title"]
            translated_body = text_fields["body"]
            status = detection.status
            was_translated = False
            error = detection.error

            if should_translate:
                translated_title, title_status, title_error = self._translate_text(
                    text_fields["title"],
                    source_language=detection.language,
                )
                translated_body, body_status, body_error = self._translate_text(
                    text_fields["body"],
                    source_language=detection.language,
                )
                errors = [value for value in (title_error, body_error) if value]
                if errors:
                    error = "; ".join(errors)
                    status = body_status if body_status != "translated" else title_status
                else:
                    status = "translated"
                    was_translated = True
                    self.translated += 1
            else:
                self.detected_only += 1
                if detection.status == "detected" and _is_english(detection.language):
                    status = "already_english"
                elif detection.status == "detected":
                    status = f"detected_only:{self.translation_provider}"

            translation_info = {
                "source_language": detection.language,
                "source_language_name": detection.language_name,
                "source_language_confidence": detection.confidence,
                "target_language": self.target_language,
                "target_language_name": "English" if self.target_language == DEFAULT_TARGET_LANGUAGE else self.target_language,
                "detector": detection.detector,
                "provider": self.translation_provider,
                "model": self._active_translation_model_name(),
                "status": status,
                "was_translated": was_translated,
                "processed_at": _utc_now(),
                "actor_id": self.actor_id,
            }
            if error:
                translation_info["error"] = error

            enriched["source_language"] = detection.language
            enriched["source_language_name"] = detection.language_name
            enriched["title_en"] = translated_title
            enriched["body_en"] = translated_body
            enriched["text_en"] = translated_body or translated_title
            enriched["translation"] = translation_info
            enriched["original_event_type"] = original_event_type
            enriched["event_type"] = "INTEL_ITEM_TRANSLATED"
            _safe_extra(enriched)["translation"] = translation_info

            return {"ok": True, "payload": enriched}
        except Exception as exc:
            self.errors += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._log.exception("Translation actor %s failed", self.actor_id)
            failed = copy.deepcopy(payload) if isinstance(payload, dict) else {}
            failed["translation"] = {
                "source_language": "und",
                "source_language_name": "Unknown",
                "target_language": self.target_language,
                "detector": "glotlid",
                "provider": self.translation_provider,
                "status": "engine_error",
                "error": self.last_error,
                "processed_at": _utc_now(),
                "actor_id": self.actor_id,
            }
            failed["event_type"] = "INTEL_ITEM_TRANSLATED"
            return {"ok": False, "reason": self.last_error, "payload": failed}

    def health(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "started_at": self.started_at,
            "received": self.received,
            "translated": self.translated,
            "detected_only": self.detected_only,
            "skipped": self.skipped,
            "errors": self.errors,
            "last_error": self.last_error,
            "detector_loaded": self._detector is not None,
            "detector_model_path": str(self.detector_model_path),
            "translation_provider": self.translation_provider,
            "translator_loaded": self._translator_loaded,
            "translation_model": self._active_translation_model_name(),
            "target_language": self.target_language,
        }

    @property
    def _translator_loaded(self) -> bool:
        return self._translation_model is not None or self._translation_pipeline is not None

    def _load_detector(self) -> None:
        if self._detector is not None:
            return

        model_path = self.detector_model_path.expanduser().resolve()
        if not model_path.exists() and self.detector_auto_download:
            model_path = download_glotlid_model(model_path, repo_id=self.detector_repo_id)
            self.detector_model_path = model_path

        if not model_path.exists():
            self.last_error = f"GlotLID model not found: {model_path}"
            self._log.warning(self.last_error)
            return

        try:
            import fasttext

            self._detector = fasttext.load_model(str(model_path))
            self.detector_model_path = model_path
            self._log.info("GlotLID model loaded: %s", model_path)
        except Exception as exc:
            self.last_error = f"GlotLID load failed: {exc}"
            self._log.warning(self.last_error, exc_info=True)

    def _load_translator(self) -> None:
        provider = self.translation_provider
        if provider in {"", "none", "off", "disabled"}:
            return

        try:
            if provider == "nllb":
                self._load_nllb()
                return
            if provider == "opus_mt":
                self._load_opus_mt()
                return
            self.last_error = f"unsupported translation provider: {provider}"
            self._log.warning(self.last_error)
        except Exception as exc:
            self.last_error = f"{provider} load failed: {exc}"
            self._log.warning(self.last_error, exc_info=True)

    def _load_nllb(self) -> None:
        if self._translation_model is not None:
            return

        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        model_name = self.translation_model_path or DEFAULT_NLLB_MODEL
        self._tokenizer = AutoTokenizer.from_pretrained(model_name)
        self._translation_model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

        device = self._resolve_torch_device()
        if device:
            self._translation_model.to(device)
        self._translation_model.eval()
        self._log.info("NLLB translator loaded: %s on %s", model_name, device or "default")

    def _load_opus_mt(self) -> None:
        if self._translation_pipeline is not None:
            return

        from transformers import pipeline

        model_name = self.translation_model_path or DEFAULT_OPUS_MT_MODEL
        self._translation_pipeline = pipeline(
            "translation",
            model=model_name,
            tokenizer=model_name,
            device=self._resolve_pipeline_device(),
        )
        self._log.info("OPUS-MT translator loaded: %s", model_name)

    def _detect_language(self, text: str) -> DetectionResult:
        if not text.strip():
            return DetectionResult(
                language="und",
                language_name="Unknown",
                confidence=0.0,
                detector="glotlid",
                status="empty_text",
            )

        if self._detector is None:
            self._load_detector()
        if self._detector is None:
            return DetectionResult(
                language="und",
                language_name="Unknown",
                confidence=0.0,
                detector="glotlid",
                status="detector_unavailable",
                error=self.last_error,
            )

        try:
            labels, probabilities = self._detector.predict(text.replace("\n", " "), k=1)
            label = labels[0] if labels else ""
            confidence = float(probabilities[0]) if probabilities else 0.0
            language = _normalize_label(label)
            return DetectionResult(
                language=language,
                language_name=_language_name(language),
                confidence=round(confidence, 4),
                detector="glotlid",
                status="detected",
            )
        except Exception as exc:
            self.last_error = f"GlotLID predict failed: {exc}"
            return DetectionResult(
                language="und",
                language_name="Unknown",
                confidence=0.0,
                detector="glotlid",
                status="detector_error",
                error=self.last_error,
            )

    def _should_translate(self, detection: DetectionResult, text: str) -> bool:
        if not text.strip():
            return False
        if detection.status != "detected":
            return False
        if detection.confidence < self.min_detection_confidence:
            return False
        if _is_english(detection.language):
            return False
        if self.translation_provider in {"", "none", "off", "disabled"}:
            return False
        if not self._translator_loaded:
            self._load_translator()
        return self._translator_loaded

    def _translate_text(self, text: str, *, source_language: str) -> tuple[str, str, str]:
        if not text.strip():
            return "", "empty_text", ""

        provider = self.translation_provider
        try:
            if provider == "nllb":
                return self._translate_nllb(text, source_language=source_language), "translated", ""
            if provider == "opus_mt":
                return self._translate_opus_mt(text), "translated", ""
            return text, f"provider_disabled:{provider}", ""
        except Exception as exc:
            error = f"{provider} translate failed: {exc}"
            self.errors += 1
            self.last_error = error
            self._log.warning(error, exc_info=True)
            return text, "translation_error", error

    def _translate_nllb(self, text: str, *, source_language: str) -> str:
        if self._tokenizer is None or self._translation_model is None:
            raise RuntimeError("NLLB translator is not loaded")

        src_lang = _normalize_label(source_language)
        tokenizer = self._tokenizer
        model = self._translation_model
        target = self.target_language

        if hasattr(tokenizer, "src_lang"):
            tokenizer.src_lang = src_lang

        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=1024)
        device = self._current_model_device()
        if device:
            inputs = {key: value.to(device) for key, value in inputs.items()}

        forced_bos_token_id = self._lang_token_id(target)
        generated = model.generate(
            **inputs,
            forced_bos_token_id=forced_bos_token_id,
            max_new_tokens=self.nllb_max_new_tokens,
            num_beams=self.nllb_num_beams,
        )
        return tokenizer.batch_decode(generated, skip_special_tokens=True)[0].strip()

    def _translate_opus_mt(self, text: str) -> str:
        if self._translation_pipeline is None:
            raise RuntimeError("OPUS-MT translator is not loaded")
        result = self._translation_pipeline(text, max_length=512)
        if isinstance(result, list) and result:
            return str(result[0].get("translation_text") or "").strip()
        return text

    def _lang_token_id(self, lang_code: str) -> int:
        tokenizer = self._tokenizer
        if tokenizer is None:
            raise RuntimeError("tokenizer is not loaded")

        lang_code = _normalize_label(lang_code)
        lang_map = getattr(tokenizer, "lang_code_to_id", None)
        if isinstance(lang_map, dict) and lang_code in lang_map:
            return int(lang_map[lang_code])

        token_id = tokenizer.convert_tokens_to_ids(lang_code)
        unk_id = getattr(tokenizer, "unk_token_id", None)
        if token_id is None or token_id == unk_id:
            raise ValueError(f"language token not supported by tokenizer: {lang_code}")
        return int(token_id)

    def _resolve_torch_device(self) -> str:
        if self.device not in {"", "auto"}:
            return self.device
        try:
            import torch

            return "cuda" if torch.cuda.is_available() else "cpu"
        except Exception:
            return "cpu"

    def _resolve_pipeline_device(self) -> int:
        if self.device in {"", "auto"}:
            try:
                import torch

                return 0 if torch.cuda.is_available() else -1
            except Exception:
                return -1
        if self.device.startswith("cuda"):
            return 0
        return -1

    def _current_model_device(self) -> Any:
        try:
            return next(self._translation_model.parameters()).device
        except Exception:
            return None

    def _active_translation_model_name(self) -> str:
        if self.translation_provider == "nllb":
            return self.translation_model_path or DEFAULT_NLLB_MODEL
        if self.translation_provider == "opus_mt":
            return self.translation_model_path or DEFAULT_OPUS_MT_MODEL
        return ""


class TranslationEngineRay:
    """MQ consumer/producer that dispatches translation work to Ray actors."""

    def __init__(self, config: TranslationEngineConfig) -> None:
        self.config = config
        self._actors: list[Any] = []
        self._actor_index = 0
        self._inflight_sem: asyncio.Semaphore | None = None
        self._mq: Any = None
        self._owns_ray = False
        self._stop_event = asyncio.Event()
        self._health_task: asyncio.Task[None] | None = None

        self.received = 0
        self.published = 0
        self.failed = 0
        self.skipped = 0
        self.started_at = _utc_now()
        self.last_error = ""

    @classmethod
    def from_env(cls) -> "TranslationEngineRay":
        return cls(TranslationEngineConfig.from_env())

    async def run_forever(self) -> None:
        await self.start()

    async def start(self) -> None:
        await self._init_ray()
        await self._spawn_actors()
        await self._connect_mq()

        await self._mq.subscribe_event(self.config.input_topic, self._on_event)
        self._health_task = asyncio.create_task(self._health_loop(), name="translation-engine-health")
        log.info(
            "Translation engine online: input=%s output=%s actors=%d provider=%s",
            self.config.input_topic,
            self.config.output_topic or f"{self.config.output_prefix}.<domain>",
            len(self._actors),
            self.config.translation_provider,
        )
        await self._mq.start_consuming()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._health_task:
            self._health_task.cancel()
            await asyncio.gather(self._health_task, return_exceptions=True)
            self._health_task = None

        if self._mq is not None:
            try:
                await self._mq.stop_consuming()
            except Exception:
                pass
            try:
                await self._mq.disconnect()
            except Exception as exc:
                log.warning("Translation MQ disconnect failed: %s", exc, exc_info=True)
            self._mq = None

        if self._actors:
            try:
                await asyncio.gather(*[actor.stop.remote() for actor in self._actors], return_exceptions=True)
            except Exception as exc:
                log.warning("Translation actor stop failed: %s", exc, exc_info=True)
            self._actors = []

        if self._owns_ray:
            try:
                ray.shutdown()
            except Exception as exc:
                log.warning("Ray shutdown failed: %s", exc, exc_info=True)
            self._owns_ray = False

    def request_stop(self) -> None:
        self._stop_event.set()
        if self._mq is not None:
            try:
                asyncio.create_task(self._mq.stop_consuming())
            except RuntimeError:
                pass

    async def health(self) -> dict[str, Any]:
        actor_health: list[Any] = []
        if self._actors:
            try:
                actor_health = list(
                    await asyncio.gather(*[actor.health.remote() for actor in self._actors], return_exceptions=True)
                )
            except Exception as exc:
                self.last_error = f"health actors: {exc}"
        return {
            "enabled": True,
            "started_at": self.started_at,
            "input_topic": self.config.input_topic,
            "output_prefix": self.config.output_prefix,
            "output_topic": self.config.output_topic,
            "received": self.received,
            "published": self.published,
            "failed": self.failed,
            "skipped": self.skipped,
            "last_error": self.last_error,
            "actors": actor_health,
        }

    async def process_one_for_test(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._actors:
            await self._init_ray()
            await self._spawn_actors()
        result = await self._actors[0].process.remote(payload)
        return result

    async def _init_ray(self) -> None:
        if ray.is_initialized():
            self._owns_ray = False
            return

        init_kwargs: dict[str, Any] = {
            "namespace": self.config.ray_namespace,
            "ignore_reinit_error": True,
            "log_to_driver": False,
        }
        if self.config.ray_address:
            init_kwargs["address"] = self.config.ray_address
        else:
            init_kwargs["include_dashboard"] = False

        ray.init(**init_kwargs)
        self._owns_ray = True
        log.info("Ray initialized for translation engine namespace=%s", self.config.ray_namespace)

    async def _spawn_actors(self) -> None:
        if self._actors:
            return

        config_dict = asdict(self.config)
        config_dict["detector_model_path"] = str(self.config.detector_model_path)
        for index in range(self.config.num_actors):
            actor = TranslationWorkerActor.options(
                max_concurrency=self.config.actor_max_concurrency,
                name=f"translation-worker-{index}",
                get_if_exists=False,
            ).remote(
                actor_id=f"translation-worker-{index}",
                config=config_dict,
            )
            self._actors.append(actor)

        self._inflight_sem = asyncio.Semaphore(
            self.config.num_actors * self.config.max_inflight_per_actor
        )
        await asyncio.gather(*[actor.start.remote() for actor in self._actors])
        log.info("Translation actor pool ready: %d actors", len(self._actors))

    async def _connect_mq(self) -> None:
        if RabbitMQProviderAsync is None:
            raise RuntimeError("RabbitMQProviderAsync could not be imported")

        host = os.getenv("MQ_HOST", os.getenv("RABBITMQ_HOST", "localhost"))
        port = _int_env("MQ_PORT", int(os.getenv("RABBITMQ_PORT", "5672")), minimum=1)
        username = os.getenv("MQ_USER", os.getenv("RABBITMQ_USER", "guest"))
        password = os.getenv("MQ_PASSWORD", os.getenv("RABBITMQ_PASSWORD", "guest"))

        provider = os.getenv("MQ_PROVIDER", "RABBITMQ_ASYNC").strip().upper()
        if provider not in {"RABBITMQ", "RABBITMQ_ASYNC"}:
            raise RuntimeError(f"TranslationEngineRay supports RabbitMQ async only; MQ_PROVIDER={provider!r}")

        self._mq = RabbitMQProviderAsync(host=host, port=port, username=username, password=password)
        await self._mq.connect()
        log.info("Translation MQ connected to %s:%s", host, port)

    async def _on_event(self, payload: dict[str, Any]) -> None:
        if self._stop_event.is_set():
            return
        if not self._actors:
            self.failed += 1
            self.last_error = "no actors available"
            return

        self.received += 1
        semaphore = self._inflight_sem
        if semaphore is None:
            self.failed += 1
            self.last_error = "inflight semaphore is not initialized"
            return

        async with semaphore:
            actor = self._next_actor()
            result = await actor.process.remote(payload)
            if result.get("skipped"):
                self.skipped += 1
                return
            translated_payload = result.get("payload") or {}
            if not isinstance(translated_payload, dict):
                self.failed += 1
                self.last_error = f"actor returned invalid payload: {result!r}"
                return
            await self._publish(translated_payload)

    def _next_actor(self) -> Any:
        actor = self._actors[self._actor_index % len(self._actors)]
        self._actor_index += 1
        return actor

    async def _publish(self, payload: dict[str, Any]) -> None:
        try:
            topic = _topic_for_payload(
                self.config.output_prefix,
                payload,
                fixed_topic=self.config.output_topic,
            )
            ok = await self._mq.publish_event(topic, payload)
            if ok:
                self.published += 1
            else:
                self.failed += 1
                self.last_error = f"publish returned false for topic={topic}"
        except Exception as exc:
            self.failed += 1
            self.last_error = f"publish failed: {exc}"
            log.warning("Translation publish failed: %s", exc, exc_info=True)

    async def _health_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.config.health_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
            if self._stop_event.is_set():
                break
            try:
                payload = {
                    "event_type": "TRANSLATION_ENGINE_HEALTH",
                    "timestamp": _utc_now(),
                    **(await self.health()),
                }
                await self._mq.publish_event(self.config.health_topic, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"health publish failed: {exc}"
                log.warning("Translation health publish failed: %s", exc, exc_info=True)


def _load_environment() -> None:
    load_dotenv(SERVICE_ROOT / ".env")
    load_dotenv(PACKAGE_ROOT / ".env")
    load_dotenv(EXTRACTOR_DIR / "telegram_session.env")
    load_dotenv(EXTRACTOR_DIR / "translation_engine.env")
    load_dotenv()

    try:
        from forex_shared.env_config_manager import EnvConfigManager

        EnvConfigManager.startup()
    except Exception as exc:
        log.warning("EnvConfigManager.startup() failed; using direct MQ env: %s", exc)


def _synthetic_payload(text: str) -> dict[str, Any]:
    return {
        "event_type": "INTEL_ITEM",
        "id": f"translation-dry-run:{abs(hash(text))}",
        "source": "dry_run",
        "domain": "social",
        "title": _compact_text(text, limit=240),
        "body": text,
        "url": "",
        "published_at": _utc_now(),
        "ts": _utc_now(),
        "fetched_at": _utc_now(),
        "country": [],
        "severity": "",
        "tags": ["dry_run", "translation"],
        "extra": {"dry_run": True},
    }


async def _run_dry(args: argparse.Namespace, config: TranslationEngineConfig) -> None:
    engine = TranslationEngineRay(config)
    try:
        text = args.dry_run_text or "Hola mundo. El mercado espera la decision del banco central."
        result = await engine.process_one_for_test(_synthetic_payload(text))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        await engine.stop()


async def _run_service(config: TranslationEngineConfig) -> None:
    engine = TranslationEngineRay(config)
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        log.info("Translation engine shutdown requested")
        engine.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_request_stop))

    try:
        await engine.run_forever()
    finally:
        await engine.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Ray/MQ OSINT language detection and translation engine.",
    )
    parser.add_argument("--input-topic", default="", help="MQ topic to consume. Default: env/default intel.events.#")
    parser.add_argument("--output-prefix", default="", help="MQ output prefix. Default: env/default intel.translated")
    parser.add_argument("--output-topic", default="", help="Fixed MQ output topic, overriding output prefix/domain routing.")
    parser.add_argument("--actors", type=int, default=0, help="Number of Ray translation actors.")
    parser.add_argument("--provider", default="", help="Translation provider: none, nllb, opus_mt.")
    parser.add_argument("--model-path", default="", help="Translation model path or Hugging Face model id.")
    parser.add_argument("--glotlid-model", default="", help="Path to GlotLID fastText model.bin.")
    parser.add_argument("--download-glotlid", action="store_true", help="Download GlotLID model.bin and exit.")
    parser.add_argument("--auto-download-glotlid", action="store_true", help="Allow actors to download GlotLID if missing.")
    parser.add_argument("--dry-run-text", default="", help="Process one synthetic message and print the enriched payload.")
    return parser.parse_args(argv)


def _config_from_args(args: argparse.Namespace) -> TranslationEngineConfig:
    config = TranslationEngineConfig.from_env()
    updates: dict[str, Any] = {}
    if args.input_topic:
        updates["input_topic"] = args.input_topic
    if args.output_prefix:
        updates["output_prefix"] = args.output_prefix
    if args.output_topic:
        updates["output_topic"] = args.output_topic
    if args.actors and args.actors > 0:
        updates["num_actors"] = args.actors
    if args.provider:
        updates["translation_provider"] = args.provider.strip().lower()
    if args.model_path:
        updates["translation_model_path"] = args.model_path
    if args.glotlid_model:
        updates["detector_model_path"] = Path(args.glotlid_model)
    if args.auto_download_glotlid:
        updates["detector_auto_download"] = True
    return replace(config, **updates)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    _load_environment()
    args = parse_args(argv)
    config = _config_from_args(args)

    if args.download_glotlid:
        path = download_glotlid_model(config.detector_model_path, repo_id=config.detector_repo_id)
        print(f"GlotLID model ready: {path}")
        return

    try:
        if args.dry_run_text:
            asyncio.run(_run_dry(args, config))
        else:
            asyncio.run(_run_service(config))
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as exc:
        log.exception("Translation engine failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
