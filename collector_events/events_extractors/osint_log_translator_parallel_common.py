"""Shared block-parallel helpers for OSINT log translation scripts."""

from __future__ import annotations

import argparse
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collector_events.events_extractors import osint_log_translator as base


DEFAULT_WORKERS = os.cpu_count() or 1


@dataclass(frozen=True)
class TranslatorRunConfig:
    log_path: Path
    lid_model: Path
    model_path: Path
    device: str
    min_confidence: float
    max_input_tokens: int
    max_new_tokens: int
    num_beams: int
    torch_threads: int
    limit: int
    dry_run: bool
    quiet: bool
    workers: int


@dataclass(frozen=True)
class LogMessageTask:
    task_id: int
    message_text: str


@dataclass(frozen=True)
class InsertTranslation:
    task_id: int


@dataclass(frozen=True)
class LogTranslationResult:
    task_id: int
    section_lines: list[str]
    status: str


@dataclass(frozen=True)
class ParsedLog:
    units: list[str | InsertTranslation]
    tasks: list[LogMessageTask]
    blocks_seen: int


_WORKER_STATE = threading.local()


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--log-path", type=Path, default=base._env_path("OSINT_LOG_PATH", base.DEFAULT_LOG_PATH))
    parser.add_argument("--lid-model", type=Path, default=base._env_path("GLOTLID_MODEL_PATH", base.DEFAULT_LID_MODEL))
    parser.add_argument(
        "--model-path",
        type=Path,
        default=base._env_path("TRANSLATION_MODEL_PATH", base.DEFAULT_NLLB_MODEL),
    )
    parser.add_argument("--device", default=os.getenv("TRANSLATION_DEVICE", "cpu"))
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=base._env_float("TRANSLATION_MIN_DETECTION_CONFIDENCE", 0.35),
    )
    parser.add_argument(
        "--max-input-tokens",
        type=int,
        default=base._env_int("TRANSLATION_NLLB_MAX_INPUT_TOKENS", 900),
    )
    parser.add_argument(
        "--max-new-tokens",
        type=int,
        default=base._env_int("TRANSLATION_NLLB_MAX_NEW_TOKENS", 768),
    )
    parser.add_argument(
        "--num-beams",
        type=int,
        default=base._env_int("TRANSLATION_NLLB_NUM_BEAMS", 4),
    )
    parser.add_argument("--torch-threads", type=int, default=base._env_int("TRANSLATION_TORCH_THREADS", 0, minimum=0))
    parser.add_argument("--limit", type=int, default=0, help="Translate only the first N message blocks.")
    parser.add_argument("--dry-run", action="store_true", help="Run translation without writing the log file.")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output and final summary.")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel workers. Default: os.cpu_count()={DEFAULT_WORKERS}.",
    )


def config_from_args(args: argparse.Namespace) -> TranslatorRunConfig:
    return TranslatorRunConfig(
        log_path=args.log_path.expanduser().resolve(),
        lid_model=args.lid_model.expanduser().resolve(),
        model_path=args.model_path.expanduser().resolve(),
        device=args.device,
        min_confidence=args.min_confidence,
        max_input_tokens=args.max_input_tokens,
        max_new_tokens=args.max_new_tokens,
        num_beams=args.num_beams,
        torch_threads=args.torch_threads,
        limit=args.limit,
        dry_run=args.dry_run,
        quiet=args.quiet,
        workers=max(1, args.workers),
    )


def parse_log_for_parallel(lines: list[str], *, limit: int) -> ParsedLog:
    units: list[str | InsertTranslation] = []
    tasks: list[LogMessageTask] = []
    index = 0
    blocks_seen = 0

    while index < len(lines):
        line = lines[index]
        units.append(line)
        index += 1

        if line.strip() != base.MESSAGE_MARKER:
            continue

        blocks_seen += 1
        process_this = limit <= 0 or blocks_seen <= limit
        message_lines: list[str] = []

        while index < len(lines) and not base._is_separator(lines[index]):
            if process_this and base._is_translation_marker(lines[index]):
                while index < len(lines) and not base._is_separator(lines[index]):
                    index += 1
                break

            message_lines.append(lines[index])
            index += 1

        if not process_this:
            units.extend(message_lines)
            continue

        while message_lines and not message_lines[-1].strip():
            message_lines.pop()

        task_id = len(tasks)
        units.extend(message_lines)
        units.append(InsertTranslation(task_id=task_id))
        tasks.append(LogMessageTask(task_id=task_id, message_text=base._message_text(message_lines)))

    return ParsedLog(units=units, tasks=tasks, blocks_seen=blocks_seen)


def render_translated_log(parsed: ParsedLog, results: list[LogTranslationResult]) -> tuple[list[str], base.TranslationStats]:
    by_id = {result.task_id: result for result in results}
    output: list[str] = []

    for unit in parsed.units:
        if isinstance(unit, str):
            output.append(unit)
            continue
        result = by_id[unit.task_id]
        output.extend(result.section_lines)

    stats = base.TranslationStats(
        blocks_seen=parsed.blocks_seen,
        blocks_processed=len(results),
        translated=sum(1 for result in results if result.status == "translated"),
        already_english=sum(1 for result in results if result.status == "already_english"),
        unavailable=sum(1 for result in results if result.status == "unavailable"),
    )
    return output, stats


def _detector(config: TranslatorRunConfig) -> base.FastTextLanguageDetector:
    detector = getattr(_WORKER_STATE, "detector", None)
    if detector is None:
        detector = base.FastTextLanguageDetector(config.lid_model)
        _WORKER_STATE.detector = detector
    return detector


def _translator_factory(config: TranslatorRunConfig) -> base.NllbTranslator:
    translator = getattr(_WORKER_STATE, "translator", None)
    if translator is None:
        translator = base.NllbTranslator(
            config.model_path,
            device=config.device,
            max_input_tokens=config.max_input_tokens,
            max_new_tokens=config.max_new_tokens,
            num_beams=config.num_beams,
            torch_threads=config.torch_threads,
        )
        _WORKER_STATE.translator = translator
    return translator


def process_message_task(task: LogMessageTask, config: TranslatorRunConfig) -> LogTranslationResult:
    base._configure_transformers_output(quiet=config.quiet)
    section_lines, status = base._build_translation_section(
        task.message_text,
        detector=_detector(config),
        translator_factory=lambda: _translator_factory(config),
        min_confidence=config.min_confidence,
    )
    return LogTranslationResult(task_id=task.task_id, section_lines=section_lines, status=status)


def result_from_payload(payload: dict[str, Any]) -> LogTranslationResult:
    return LogTranslationResult(
        task_id=int(payload["task_id"]),
        section_lines=list(payload["section_lines"]),
        status=str(payload["status"]),
    )


def result_to_payload(result: LogTranslationResult) -> dict[str, Any]:
    return {
        "task_id": result.task_id,
        "section_lines": result.section_lines,
        "status": result.status,
    }


def task_from_payload(payload: dict[str, Any]) -> LogMessageTask:
    return LogMessageTask(task_id=int(payload["task_id"]), message_text=str(payload["message_text"]))


def task_to_payload(task: LogMessageTask) -> dict[str, Any]:
    return {"task_id": task.task_id, "message_text": task.message_text}


def config_from_payload(payload: dict[str, Any]) -> TranslatorRunConfig:
    return TranslatorRunConfig(
        log_path=Path(payload["log_path"]),
        lid_model=Path(payload["lid_model"]),
        model_path=Path(payload["model_path"]),
        device=str(payload["device"]),
        min_confidence=float(payload["min_confidence"]),
        max_input_tokens=int(payload["max_input_tokens"]),
        max_new_tokens=int(payload["max_new_tokens"]),
        num_beams=int(payload["num_beams"]),
        torch_threads=int(payload["torch_threads"]),
        limit=int(payload["limit"]),
        dry_run=bool(payload["dry_run"]),
        quiet=bool(payload["quiet"]),
        workers=int(payload["workers"]),
    )


def config_to_payload(config: TranslatorRunConfig) -> dict[str, Any]:
    return {
        "log_path": str(config.log_path),
        "lid_model": str(config.lid_model),
        "model_path": str(config.model_path),
        "device": config.device,
        "min_confidence": config.min_confidence,
        "max_input_tokens": config.max_input_tokens,
        "max_new_tokens": config.max_new_tokens,
        "num_beams": config.num_beams,
        "torch_threads": config.torch_threads,
        "limit": config.limit,
        "dry_run": config.dry_run,
        "quiet": config.quiet,
        "workers": config.workers,
    }


def read_parsed_log(config: TranslatorRunConfig) -> tuple[list[str], str, bool, ParsedLog]:
    if not config.log_path.exists():
        raise FileNotFoundError(f"log file not found: {config.log_path}")

    lines, newline, had_trailing_newline = base._read_lines(config.log_path)
    parsed = parse_log_for_parallel(lines, limit=config.limit)
    return lines, newline, had_trailing_newline, parsed


def write_and_report(
    *,
    backend: str,
    config: TranslatorRunConfig,
    parsed: ParsedLog,
    results: list[LogTranslationResult],
    newline: str,
    had_trailing_newline: bool,
) -> None:
    translated_lines, stats = render_translated_log(parsed, results)

    if not config.dry_run:
        base._write_lines(config.log_path, translated_lines, newline, had_trailing_newline)

    if not config.quiet:
        action = "would update" if config.dry_run else "updated"
        print(
            f"{action} {config.log_path} via {backend}: "
            f"seen={stats.blocks_seen}, processed={stats.blocks_processed}, "
            f"translated={stats.translated}, english={stats.already_english}, "
            f"unavailable={stats.unavailable}, workers={config.workers}"
        )
