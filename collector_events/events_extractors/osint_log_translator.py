"""Translate Telegram OSINT dashboard logs in place.

This is intentionally standalone: no Ray, no MQ. It reads each Telegram block,
extracts the text below the "💬 Message" marker, detects the source language
with the local fastText LID model, translates non-English messages with the
local NLLB-200 model, and inserts an English translation section into the same
log file.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


THIS_FILE = Path(__file__).resolve()
EXTRACTOR_DIR = THIS_FILE.parent
SERVICE_ROOT = THIS_FILE.parents[2]

DEFAULT_LOG_PATH = EXTRACTOR_DIR / "telegram_osint_dashboard_cleaned-0.log"
DEFAULT_LID_MODEL = SERVICE_ROOT / ".models" / "lid" / "lid.176.bin"
DEFAULT_NLLB_MODEL = SERVICE_ROOT / ".models" / "nllb-200-distilled-600M"

MESSAGE_MARKER = "💬 Message"
TRANSLATION_MARKER_PREFIX = "⚡Translation from "
NO_TRANSLATION_MARKER = "➡️No translation needed"
TARGET_LANGUAGE = "eng_Latn"
ENGLISH_CODES = {"en", "eng", "eng_latn", "eng_Latn", "english"}

ISO2_TO_NLLB = {
    "ar": "arb_Arab",
    "bg": "bul_Cyrl",
    "cs": "ces_Latn",
    "da": "dan_Latn",
    "de": "deu_Latn",
    "el": "ell_Grek",
    "en": "eng_Latn",
    "es": "spa_Latn",
    "et": "est_Latn",
    "fa": "pes_Arab",
    "fi": "fin_Latn",
    "fr": "fra_Latn",
    "he": "heb_Hebr",
    "hi": "hin_Deva",
    "hr": "hrv_Latn",
    "hu": "hun_Latn",
    "id": "ind_Latn",
    "it": "ita_Latn",
    "ja": "jpn_Jpan",
    "ko": "kor_Hang",
    "lt": "lit_Latn",
    "lv": "lvs_Latn",
    "nl": "nld_Latn",
    "no": "nob_Latn",
    "pl": "pol_Latn",
    "pt": "por_Latn",
    "ro": "ron_Latn",
    "ru": "rus_Cyrl",
    "sk": "slk_Latn",
    "sl": "slv_Latn",
    "sr": "srp_Cyrl",
    "sv": "swe_Latn",
    "th": "tha_Thai",
    "tr": "tur_Latn",
    "uk": "ukr_Cyrl",
    "vi": "vie_Latn",
    "zh": "zho_Hans",
}

LANGUAGE_NAMES = {
    "arb_Arab": "Arabic",
    "bul_Cyrl": "Bulgarian",
    "ces_Latn": "Czech",
    "dan_Latn": "Danish",
    "deu_Latn": "German",
    "ell_Grek": "Greek",
    "eng_Latn": "English",
    "spa_Latn": "Spanish",
    "est_Latn": "Estonian",
    "pes_Arab": "Persian",
    "fin_Latn": "Finnish",
    "fra_Latn": "French",
    "heb_Hebr": "Hebrew",
    "hin_Deva": "Hindi",
    "hrv_Latn": "Croatian",
    "hun_Latn": "Hungarian",
    "ind_Latn": "Indonesian",
    "ita_Latn": "Italian",
    "jpn_Jpan": "Japanese",
    "kor_Hang": "Korean",
    "lit_Latn": "Lithuanian",
    "lvs_Latn": "Latvian",
    "nld_Latn": "Dutch",
    "nob_Latn": "Norwegian Bokmal",
    "pol_Latn": "Polish",
    "por_Latn": "Portuguese",
    "ron_Latn": "Romanian",
    "rus_Cyrl": "Russian",
    "slk_Latn": "Slovak",
    "slv_Latn": "Slovenian",
    "srp_Cyrl": "Serbian",
    "swe_Latn": "Swedish",
    "tha_Thai": "Thai",
    "tur_Latn": "Turkish",
    "ukr_Cyrl": "Ukrainian",
    "vie_Latn": "Vietnamese",
    "zho_Hans": "Chinese",
}


@dataclass(frozen=True)
class DetectionResult:
    raw_code: str
    nllb_code: str
    language_name: str
    confidence: float

    @property
    def is_english(self) -> bool:
        return self.nllb_code == TARGET_LANGUAGE or self.raw_code.lower() in ENGLISH_CODES

    @property
    def display_name(self) -> str:
        if self.nllb_code == "und":
            return "Unknown"
        return f"{self.language_name} ({self.nllb_code})"


@dataclass
class TranslationStats:
    blocks_seen: int = 0
    blocks_processed: int = 0
    translated: int = 0
    already_english: int = 0
    unavailable: int = 0


def _load_dotenv_files() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv(EXTRACTOR_DIR / "translation_engine.env")


def _configure_transformers_output(*, quiet: bool) -> None:
    os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
    if quiet:
        os.environ["OSINT_SUPPRESS_FASTTEXT_PROGRESS"] = "1"

    try:
        from transformers.utils import logging as transformers_logging

        transformers_logging.set_verbosity_error()
        if quiet:
            transformers_logging.disable_progress_bar()
    except Exception:
        pass

    if quiet:
        try:
            from huggingface_hub.utils import disable_progress_bars

            disable_progress_bars()
        except Exception:
            pass


@contextlib.contextmanager
def _suppress_native_output(enabled: bool):
    if not enabled:
        yield
        return

    saved_fds: list[tuple[int, int]] = []
    devnull_fd: int | None = None

    try:
        fds: list[int] = []
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
                fd = stream.fileno()
            except Exception:
                continue
            if fd not in fds:
                fds.append(fd)

        if not fds:
            yield
            return

        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        for fd in fds:
            saved_fd = os.dup(fd)
            saved_fds.append((fd, saved_fd))
            os.dup2(devnull_fd, fd)

        yield
    finally:
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass
        for fd, saved_fd in reversed(saved_fds):
            try:
                os.dup2(saved_fd, fd)
            finally:
                os.close(saved_fd)
        if devnull_fd is not None:
            os.close(devnull_fd)


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name, "").strip().strip('"')
    return Path(value) if value else default


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except ValueError:
        return default


def _normalize_label(label: str) -> tuple[str, str]:
    raw = str(label or "").strip().removeprefix("__label__").replace("-", "_")
    lower = raw.lower()
    if not raw:
        return "und", "und"
    if lower in ENGLISH_CODES:
        return raw, TARGET_LANGUAGE
    return raw, ISO2_TO_NLLB.get(lower, raw)


def _language_name(nllb_code: str, raw_code: str) -> str:
    if nllb_code in LANGUAGE_NAMES:
        return LANGUAGE_NAMES[nllb_code]

    alpha = nllb_code.split("_", 1)[0] if nllb_code != "und" else raw_code
    try:
        import pycountry

        lang = pycountry.languages.get(alpha_3=alpha)
        if lang is None and len(alpha) == 2:
            lang = pycountry.languages.get(alpha_2=alpha)
        if lang is not None:
            return getattr(lang, "name", nllb_code)
    except Exception:
        pass

    return "Unknown" if nllb_code == "und" else nllb_code


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _looks_like_english(text: str) -> bool:
    letters = [char for char in text if char.isalpha()]
    if not letters:
        return False
    ascii_letters = sum(1 for char in letters if ord(char) < 128)
    return ascii_letters / len(letters) >= 0.9


def _is_separator(line: str) -> bool:
    stripped = line.strip()
    return len(stripped) >= 20 and len(set(stripped)) == 1 and stripped[0] in {"─", "═"}


def _is_translation_marker(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith(TRANSLATION_MARKER_PREFIX) or stripped == NO_TRANSLATION_MARKER


def _message_text(message_lines: list[str]) -> str:
    cleaned = []
    for line in message_lines:
        cleaned.append(line[3:] if line.startswith("   ") else line.strip())
    return _compact_text("\n".join(cleaned))


def _wrapped_section_lines(text: str, *, width: int = 96, indent: str = "   ") -> list[str]:
    paragraphs = [part.strip() for part in re.split(r"\n{2,}", text) if part.strip()]
    if not paragraphs:
        return [f"{indent}[empty translation]"]

    lines: list[str] = []
    for index, paragraph in enumerate(paragraphs):
        if index:
            lines.append("")
        wrapped = textwrap.wrap(
            _compact_text(paragraph),
            width=width,
            initial_indent=indent,
            subsequent_indent=indent,
            break_long_words=False,
            break_on_hyphens=False,
        )
        lines.extend(wrapped or [indent])
    return lines


def _format_elapsed(seconds: float) -> str:
    if seconds >= 60:
        return f"{seconds / 60:.1f} mins"
    return f"{seconds:.1f} seconds"


class FastTextLanguageDetector:
    def __init__(self, model_path: Path, *, quiet: bool | None = None) -> None:
        if not model_path.exists():
            raise FileNotFoundError(f"language detector model not found: {model_path}")

        import fasttext

        env_suppressed = (
            os.getenv("OSINT_SUPPRESS_FASTTEXT_PROGRESS", "").strip().lower() in {"1", "true", "yes", "on"}
        )
        suppress_output = env_suppressed if quiet is None else quiet or env_suppressed
        with _suppress_native_output(suppress_output):
            self._model = fasttext.load_model(str(model_path))

    def detect(self, text: str) -> DetectionResult:
        sample = _compact_text(text)
        if not sample:
            return DetectionResult("und", "und", "Unknown", 0.0)

        # fasttext-wheel 0.9.2 calls np.array(..., copy=False) for single strings,
        # which fails on NumPy 2.x. The list path returns the same prediction safely.
        labels, probabilities = self._model.predict([sample.replace("\n", " ")], k=1)
        label = labels[0][0] if labels and labels[0] else ""
        confidence = float(probabilities[0][0]) if probabilities and len(probabilities[0]) else 0.0
        raw_code, nllb_code = _normalize_label(label)
        return DetectionResult(
            raw_code=raw_code,
            nllb_code=nllb_code,
            language_name=_language_name(nllb_code, raw_code),
            confidence=round(confidence, 4),
        )


class NllbTranslator:
    def __init__(
        self,
        model_path: Path,
        *,
        device: str,
        max_input_tokens: int,
        max_new_tokens: int,
        num_beams: int,
        torch_threads: int,
    ) -> None:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        if torch_threads > 0:
            try:
                import torch

                torch.set_num_threads(torch_threads)
            except Exception:
                pass

        local_files_only = model_path.exists()
        self._tokenizer = AutoTokenizer.from_pretrained(
            str(model_path),
            local_files_only=local_files_only,
        )
        self._model = AutoModelForSeq2SeqLM.from_pretrained(
            str(model_path),
            local_files_only=local_files_only,
        )
        self._device = self._resolve_device(device)
        if self._device:
            self._model.to(self._device)
        self._model.eval()

        self._max_input_tokens = max_input_tokens
        self._max_new_tokens = max_new_tokens
        self._num_beams = num_beams

    def translate(self, text: str, *, source_language: str) -> str:
        src_lang = self._normalize_supported_language(source_language)
        chunks = self._chunk_text(text)
        translated = [self._translate_chunk(chunk, source_language=src_lang) for chunk in chunks]
        return _compact_text(" ".join(part for part in translated if part.strip()))

    def _resolve_device(self, requested: str) -> str:
        requested = (requested or "auto").strip().lower()
        if requested not in {"", "auto"}:
            return requested

        try:
            import torch

            return "cuda" if torch.cuda.is_available() else "cpu"
        except Exception:
            return "cpu"

    def _normalize_supported_language(self, language: str) -> str:
        _, nllb_code = _normalize_label(language)
        self._lang_token_id(nllb_code)
        return nllb_code

    def _lang_token_id(self, language: str) -> int:
        tokenizer = self._tokenizer
        lang_map = getattr(tokenizer, "lang_code_to_id", None)
        if isinstance(lang_map, dict) and language in lang_map:
            return int(lang_map[language])

        token_id = tokenizer.convert_tokens_to_ids(language)
        unk_id = getattr(tokenizer, "unk_token_id", None)
        if token_id is None or token_id == unk_id:
            raise ValueError(f"language token not supported by NLLB tokenizer: {language}")
        return int(token_id)

    def _token_count(self, text: str) -> int:
        encoded = self._tokenizer(text, add_special_tokens=False)
        return len(encoded["input_ids"])

    def _chunk_text(self, text: str) -> list[str]:
        text = _compact_text(text)
        if not text:
            return []

        sentences = re.split(r"(?<=[.!?؟。！？])\s+", text)
        chunks: list[str] = []
        current: list[str] = []

        for sentence in (part.strip() for part in sentences if part.strip()):
            proposed = _compact_text(" ".join([*current, sentence]))
            if self._token_count(proposed) <= self._max_input_tokens:
                current.append(sentence)
                continue

            if current:
                chunks.append(_compact_text(" ".join(current)))
                current = []

            if self._token_count(sentence) <= self._max_input_tokens:
                current = [sentence]
            else:
                chunks.extend(self._split_long_sentence(sentence))

        if current:
            chunks.append(_compact_text(" ".join(current)))
        return chunks

    def _split_long_sentence(self, sentence: str) -> list[str]:
        chunks: list[str] = []
        current: list[str] = []

        for word in sentence.split():
            proposed = _compact_text(" ".join([*current, word]))
            if current and self._token_count(proposed) > self._max_input_tokens:
                chunks.append(_compact_text(" ".join(current)))
                current = [word]
            else:
                current.append(word)

        if current:
            chunks.append(_compact_text(" ".join(current)))
        return chunks

    def _translate_chunk(self, text: str, *, source_language: str) -> str:
        import torch

        tokenizer = self._tokenizer
        if hasattr(tokenizer, "src_lang"):
            tokenizer.src_lang = source_language

        inputs = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=self._max_input_tokens,
        )
        if self._device:
            inputs = {key: value.to(self._device) for key, value in inputs.items()}

        with torch.no_grad():
            generated = self._model.generate(
                **inputs,
                forced_bos_token_id=self._lang_token_id(TARGET_LANGUAGE),
                max_new_tokens=self._max_new_tokens,
                num_beams=self._num_beams,
            )
        return tokenizer.batch_decode(generated, skip_special_tokens=True)[0].strip()


def _build_translation_section(
    message_text: str,
    *,
    detector: FastTextLanguageDetector,
    translator_factory: Callable[[], NllbTranslator],
    min_confidence: float,
) -> tuple[list[str], str]:
    if not message_text:
        return [NO_TRANSLATION_MARKER], "already_english"

    detection = detector.detect(message_text)
    if detection.is_english or (
        detection.confidence < min_confidence and _looks_like_english(message_text)
    ):
        return [NO_TRANSLATION_MARKER], "already_english"

    if detection.nllb_code == "und":
        return [
            f"{TRANSLATION_MARKER_PREFIX}{detection.display_name}",
            "   [translation unavailable: language detection failed]",
        ], "unavailable"

    try:
        started_at = time.perf_counter()
        translated = translator_factory().translate(
            message_text,
            source_language=detection.nllb_code,
        )
        elapsed = _format_elapsed(time.perf_counter() - started_at)
    except Exception as exc:
        elapsed = _format_elapsed(time.perf_counter() - started_at)
        return [
            f"{TRANSLATION_MARKER_PREFIX}{detection.display_name}: {elapsed}",
            f"   [translation failed: {type(exc).__name__}: {exc}]",
        ], "unavailable"

    return [
        f"{TRANSLATION_MARKER_PREFIX}{detection.display_name}: {elapsed}",
        *_wrapped_section_lines(translated),
    ], "translated"


def translate_log_lines(
    lines: list[str],
    *,
    detector: FastTextLanguageDetector,
    translator_factory: Callable[[], NllbTranslator],
    min_confidence: float,
    limit: int,
) -> tuple[list[str], TranslationStats]:
    stats = TranslationStats()
    output: list[str] = []
    index = 0

    while index < len(lines):
        line = lines[index]
        output.append(line)
        index += 1

        if line.strip() != MESSAGE_MARKER:
            continue

        stats.blocks_seen += 1
        process_this = limit <= 0 or stats.blocks_seen <= limit
        message_lines: list[str] = []

        while index < len(lines) and not _is_separator(lines[index]):
            if process_this and _is_translation_marker(lines[index]):
                while index < len(lines) and not _is_separator(lines[index]):
                    index += 1
                break

            message_lines.append(lines[index])
            index += 1

        if not process_this:
            output.extend(message_lines)
            continue

        while message_lines and not message_lines[-1].strip():
            message_lines.pop()

        output.extend(message_lines)
        section_lines, status = _build_translation_section(
            _message_text(message_lines),
            detector=detector,
            translator_factory=translator_factory,
            min_confidence=min_confidence,
        )
        output.extend(section_lines)
        stats.blocks_processed += 1
        if status == "translated":
            stats.translated += 1
        elif status == "already_english":
            stats.already_english += 1
        else:
            stats.unavailable += 1

    return output, stats


def _read_lines(path: Path) -> tuple[list[str], str, bool]:
    text = path.read_text(encoding="utf-8-sig")
    newline = "\r\n" if "\r\n" in text else "\n"
    return text.splitlines(), newline, text.endswith(("\n", "\r\n"))


def _write_lines(path: Path, lines: list[str], newline: str, had_trailing_newline: bool) -> None:
    text = newline.join(lines)
    if had_trailing_newline:
        text += newline
    path.write_text(text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Translate Telegram OSINT dashboard log message blocks in place.",
    )
    parser.add_argument("--log-path", type=Path, default=_env_path("OSINT_LOG_PATH", DEFAULT_LOG_PATH))
    parser.add_argument("--lid-model", type=Path, default=_env_path("GLOTLID_MODEL_PATH", DEFAULT_LID_MODEL))
    parser.add_argument(
        "--model-path",
        type=Path,
        default=_env_path("TRANSLATION_MODEL_PATH", DEFAULT_NLLB_MODEL),
    )
    parser.add_argument("--device", default=os.getenv("TRANSLATION_DEVICE", "cpu"))
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=_env_float("TRANSLATION_MIN_DETECTION_CONFIDENCE", 0.35),
    )
    parser.add_argument(
        "--max-input-tokens",
        type=int,
        default=_env_int("TRANSLATION_NLLB_MAX_INPUT_TOKENS", 900),
    )
    parser.add_argument(
        "--max-new-tokens",
        type=int,
        default=_env_int("TRANSLATION_NLLB_MAX_NEW_TOKENS", 768),
    )
    parser.add_argument(
        "--num-beams",
        type=int,
        default=_env_int("TRANSLATION_NLLB_NUM_BEAMS", 4),
    )
    parser.add_argument("--torch-threads", type=int, default=_env_int("TRANSLATION_TORCH_THREADS", 0, minimum=0))
    parser.add_argument("--limit", type=int, default=0, help="Translate only the first N message blocks.")
    parser.add_argument("--dry-run", action="store_true", help="Run translation without writing the log file.")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output and final summary.")
    return parser.parse_args()


def main() -> None:
    _load_dotenv_files()
    args = parse_args()
    _configure_transformers_output(quiet=args.quiet)

    log_path = args.log_path.expanduser().resolve()
    if not log_path.exists():
        raise FileNotFoundError(f"log file not found: {log_path}")

    detector = FastTextLanguageDetector(args.lid_model.expanduser().resolve(), quiet=args.quiet)
    translator: NllbTranslator | None = None

    def translator_factory() -> NllbTranslator:
        nonlocal translator
        if translator is None:
            translator = NllbTranslator(
                args.model_path.expanduser().resolve(),
                device=args.device,
                max_input_tokens=args.max_input_tokens,
                max_new_tokens=args.max_new_tokens,
                num_beams=args.num_beams,
                torch_threads=args.torch_threads,
            )
        return translator

    lines, newline, had_trailing_newline = _read_lines(log_path)
    translated_lines, stats = translate_log_lines(
        lines,
        detector=detector,
        translator_factory=translator_factory,
        min_confidence=args.min_confidence,
        limit=args.limit,
    )

    if not args.dry_run:
        _write_lines(log_path, translated_lines, newline, had_trailing_newline)

    if not args.quiet:
        action = "would update" if args.dry_run else "updated"
        print(
            f"{action} {log_path}: "
            f"seen={stats.blocks_seen}, processed={stats.blocks_processed}, "
            f"translated={stats.translated}, english={stats.already_english}, "
            f"unavailable={stats.unavailable}"
        )


if __name__ == "__main__":
    main()
