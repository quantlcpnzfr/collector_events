from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from dataclasses import dataclass, asdict

# Optional imports for models
try:
    import fasttext
except ImportError:
    fasttext = None

try:
    import torch
except ImportError:
    torch = None

from forex_shared.logging.get_logger import get_logger

log = get_logger(__name__)

DEFAULT_TARGET_LANGUAGE = "eng_Latn"
ENGLISH_CODES = {"en", "eng", "eng_latn", "eng_Latn", "english"}

ISO2_TO_NLLB = {
    "ar": "arb_Arab", "de": "deu_Latn", "el": "ell_Grek", "en": "eng_Latn",
    "es": "spa_Latn", "fa": "pes_Arab", "fr": "fra_Latn", "he": "heb_Hebr",
    "hi": "hin_Deva", "it": "ita_Latn", "ja": "jpn_Jpan", "ko": "kor_Hang",
    "pl": "pol_Latn", "pt": "por_Latn", "ru": "rus_Cyrl", "tr": "tur_Latn",
    "uk": "ukr_Cyrl", "zh": "zho_Hans",
}

@dataclass(frozen=True)
class DetectionResult:
    language: str
    language_name: str
    confidence: float
    detector: str
    status: str
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class TranslationEngine:
    """
    Core engine for language detection and translation.
    Encapsulates FastText (GlotLID) and Transformers (NLLB/OPUS-MT).
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.detector_model_path = Path(config.get("detector_model_path", ""))
        self.translation_provider = str(config.get("translation_provider", "none")).lower()
        self.translation_model_path = config.get("translation_model_path", "")
        self.target_language = config.get("target_language", DEFAULT_TARGET_LANGUAGE)
        self.max_text_chars = int(config.get("max_text_chars", 4000))
        self.device = config.get("device", "auto")
        self.min_detection_confidence = float(config.get("min_detection_confidence", 0.35))
        
        self._detector = None
        self._tokenizer = None
        self._translation_model = None
        self._translation_pipeline = None
        self._detector_load_attempted = False
        self._translator_load_attempted = False

        self.last_error = ""

    def load(self):
        """Load models into memory."""
        self._load_detector()
        self._load_translator()

    def _load_detector(self):
        if self._detector is not None or self._detector_load_attempted or not fasttext:
            return
        self._detector_load_attempted = True

        model_path = self.detector_model_path.expanduser().resolve()
        if not model_path.exists():
            log.warning(f"Language detector model not found: {model_path}")
            self.last_error = f"Detector model not found: {model_path}"
            return

        try:
            self._detector = fasttext.load_model(str(model_path))
            log.info(f"Language detector (GlotLID) loaded from {model_path}")
        except Exception as e:
            self.last_error = f"Detector load failed: {e}"
            log.error(self.last_error)

    def _load_translator(self):
        if self._translator_load_attempted:
            return
        self._translator_load_attempted = True

        if self.translation_provider in {"none", "off", "disabled"}:
            return

        try:
            from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline
            
            if self.translation_provider == "nllb":
                model_name = self.translation_model_path or "facebook/nllb-200-distilled-600M"
                self._tokenizer = AutoTokenizer.from_pretrained(model_name)
                self._translation_model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
                
                device = self._resolve_torch_device()
                if device and self._translation_model:
                    self._translation_model.to(device)
                if self._translation_model:
                    self._translation_model.eval()
                log.info(f"NLLB translator loaded: {model_name} on {device or 'cpu'}")
                
            elif self.translation_provider == "opus_mt":
                model_name = self.translation_model_path or "Helsinki-NLP/opus-mt-mul-en"
                self._translation_pipeline = pipeline(
                    "translation",
                    model=model_name,
                    tokenizer=model_name,
                    device=self._resolve_pipeline_device()
                )
                log.info(f"OPUS-MT translator loaded: {model_name}")
        except Exception as e:
            self.last_error = f"Translator load failed: {e}"
            log.error(self.last_error)

    def detect_language(self, text: str) -> DetectionResult:
        if not text.strip():
            return DetectionResult("und", "Unknown", 0.0, "glotlid", "empty_text")

        if self._detector is None:
            self._load_detector()
        
        if self._detector is None:
            return DetectionResult("und", "Unknown", 0.0, "glotlid", "unavailable", self.last_error)

        try:
            labels, probabilities = self._detector.predict([text.replace("\n", " ")], k=1)
            label = labels[0][0] if labels and labels[0] else ""
            confidence = float(probabilities[0][0]) if probabilities and len(probabilities[0]) else 0.0
            
            # Normalize label
            raw = str(label or "").strip().removeprefix("__label__").replace("-", "_")
            lower = raw.lower()
            
            language = DEFAULT_TARGET_LANGUAGE if lower in ENGLISH_CODES else ISO2_TO_NLLB.get(lower, raw)
            
            return DetectionResult(
                language=language,
                language_name=self._get_language_name(language),
                confidence=round(confidence, 4),
                detector="glotlid",
                status="detected"
            )
        except Exception as e:
            return DetectionResult("und", "Unknown", 0.0, "glotlid", "error", str(e))

    def translate(self, text: str, source_language: str) -> Tuple[str, str, str]:
        """Returns (translated_text, status, error)"""
        if not text.strip():
            return "", "empty_text", ""

        if self.translation_provider in {"none", "off", "disabled"}:
            return text, "provider_disabled", ""

        try:
            if self.translation_provider == "nllb":
                return self._translate_nllb(text, source_language), "translated", ""
            elif self.translation_provider == "opus_mt":
                return self._translate_opus_mt(text), "translated", ""
        except Exception as e:
            return text, "error", str(e)

        return text, "unsupported_provider", ""

    def _translate_nllb(self, text: str, source_language: str) -> str:
        if not self._tokenizer or not self._translation_model:
            raise RuntimeError("NLLB models not loaded")
            
        if hasattr(self._tokenizer, "src_lang"):
            self._tokenizer.src_lang = source_language

        inputs = self._tokenizer(text, return_tensors="pt", truncation=True, max_length=1024)
        
        # Move to device
        device = next(self._translation_model.parameters()).device
        inputs = {k: v.to(device) for k, v in inputs.items()}

        forced_bos_token_id = self._get_lang_token_id(self.target_language)
        
        generated = self._translation_model.generate(
            **inputs,
            forced_bos_token_id=forced_bos_token_id,
            max_new_tokens=int(self.config.get("nllb_max_new_tokens", 512)),
            num_beams=int(self.config.get("nllb_num_beams", 2))
        )
        return self._tokenizer.batch_decode(generated, skip_special_tokens=True)[0].strip()

    def _translate_opus_mt(self, text: str) -> str:
        if not self._translation_pipeline:
            raise RuntimeError("OPUS-MT pipeline not loaded")
        result = self._translation_pipeline(text, max_length=512)
        if isinstance(result, list) and result:
            return str(result[0].get("translation_text") or "").strip()
        return text

    def _get_lang_token_id(self, lang_code: str) -> int:
        lang_map = getattr(self._tokenizer, "lang_code_to_id", None)
        if isinstance(lang_map, dict) and lang_code in lang_map:
            return int(lang_map[lang_code])
        
        token_id = self._tokenizer.convert_tokens_to_ids(lang_code)
        if token_id == self._tokenizer.unk_token_id:
            raise ValueError(f"Language token {lang_code} not supported")
        return int(token_id)

    def _get_language_name(self, code: str) -> str:
        if code == DEFAULT_TARGET_LANGUAGE: return "English"
        alpha = code.split("_", 1)[0]
        try:
            import pycountry
            lang = pycountry.languages.get(alpha_3=alpha) or pycountry.languages.get(alpha_2=alpha)
            if lang: return getattr(lang, "name", code)
        except: pass
        return code

    def _resolve_torch_device(self) -> str:
        if self.device not in {"", "auto"}: return self.device
        return "cuda" if torch and torch.cuda.is_available() else "cpu"

    def _resolve_pipeline_device(self) -> int:
        if self.device in {"", "auto"}:
            return 0 if torch and torch.cuda.is_available() else -1
        return 0 if self.device.startswith("cuda") else -1
