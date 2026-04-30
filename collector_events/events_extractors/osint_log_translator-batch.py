"""Batched OSINT log translator.

This is usually the best CPU/GPU performance path for NLLB: it parses message
blocks once, detects language once per block, groups translation chunks by
source language, and sends batches through a single loaded model while keeping
the final log output in the original order.
"""

from __future__ import annotations

import argparse
import time
from collections import defaultdict
from dataclasses import dataclass

from collector_events.events_extractors import osint_log_translator as base
from collector_events.events_extractors import osint_log_translator_parallel_common as common


@dataclass(frozen=True)
class DetectedTask:
    task: common.LogMessageTask
    detection: base.DetectionResult


@dataclass(frozen=True)
class ChunkRef:
    task_id: int
    chunk_index: int
    source_language: str
    text: str


def parse_args() -> argparse.Namespace:
    base._load_dotenv_files()
    parser = argparse.ArgumentParser(description="Translate OSINT log blocks with batched NLLB generation.")
    common.add_common_args(parser)
    parser.add_argument("--batch-size", type=int, default=4, help="NLLB generation batch size per source language.")
    return parser.parse_args()


def _translate_batch(
    translator: base.NllbTranslator,
    texts: list[str],
    *,
    source_language: str,
) -> list[str]:
    import torch

    tokenizer = translator._tokenizer
    if hasattr(tokenizer, "src_lang"):
        tokenizer.src_lang = source_language

    inputs = tokenizer(
        texts,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=translator._max_input_tokens,
    )
    if translator._device:
        inputs = {key: value.to(translator._device) for key, value in inputs.items()}

    with torch.no_grad():
        generated = translator._model.generate(
            **inputs,
            forced_bos_token_id=translator._lang_token_id(base.TARGET_LANGUAGE),
            max_new_tokens=translator._max_new_tokens,
            num_beams=translator._num_beams,
        )
    return [
        text.strip()
        for text in tokenizer.batch_decode(generated, skip_special_tokens=True)
    ]


def _build_detected_results(
    tasks: list[common.LogMessageTask],
    *,
    detector: base.FastTextLanguageDetector,
    min_confidence: float,
) -> tuple[list[DetectedTask], list[common.LogTranslationResult]]:
    translate_tasks: list[DetectedTask] = []
    immediate_results: list[common.LogTranslationResult] = []

    for task in tasks:
        message_text = task.message_text
        if not message_text:
            immediate_results.append(
                common.LogTranslationResult(
                    task_id=task.task_id,
                    section_lines=[base.NO_TRANSLATION_MARKER],
                    status="already_english",
                )
            )
            continue

        detection = detector.detect(message_text)
        if detection.is_english or (
            detection.confidence < min_confidence and base._looks_like_english(message_text)
        ):
            immediate_results.append(
                common.LogTranslationResult(
                    task_id=task.task_id,
                    section_lines=[base.NO_TRANSLATION_MARKER],
                    status="already_english",
                )
            )
            continue

        if detection.nllb_code == "und":
            immediate_results.append(
                common.LogTranslationResult(
                    task_id=task.task_id,
                    section_lines=[
                        f"{base.TRANSLATION_MARKER_PREFIX}{detection.display_name}",
                        "   [translation unavailable: language detection failed]",
                    ],
                    status="unavailable",
                )
            )
            continue

        translate_tasks.append(DetectedTask(task=task, detection=detection))

    return translate_tasks, immediate_results


def main() -> None:
    args = parse_args()
    config = common.config_from_args(args)
    batch_size = max(1, args.batch_size)
    base._configure_transformers_output(quiet=config.quiet)

    _, newline, had_trailing_newline, parsed = common.read_parsed_log(config)
    detector = base.FastTextLanguageDetector(config.lid_model, quiet=config.quiet)
    translate_tasks, results = _build_detected_results(
        parsed.tasks,
        detector=detector,
        min_confidence=config.min_confidence,
    )

    if translate_tasks:
        translator = base.NllbTranslator(
            config.model_path,
            device=config.device,
            max_input_tokens=config.max_input_tokens,
            max_new_tokens=config.max_new_tokens,
            num_beams=config.num_beams,
            torch_threads=config.torch_threads,
        )

        started_by_task = {item.task.task_id: time.perf_counter() for item in translate_tasks}
        chunk_groups: dict[str, list[ChunkRef]] = defaultdict(list)
        detections_by_task = {item.task.task_id: item.detection for item in translate_tasks}

        for item in translate_tasks:
            source_language = translator._normalize_supported_language(item.detection.nllb_code)
            chunks = translator._chunk_text(item.task.message_text)
            for chunk_index, chunk in enumerate(chunks):
                chunk_groups[source_language].append(
                    ChunkRef(
                        task_id=item.task.task_id,
                        chunk_index=chunk_index,
                        source_language=source_language,
                        text=chunk,
                    )
                )

        translated_chunks: dict[int, dict[int, str]] = defaultdict(dict)
        failed_tasks: dict[int, str] = {}

        for source_language, chunk_refs in chunk_groups.items():
            for offset in range(0, len(chunk_refs), batch_size):
                batch_refs = chunk_refs[offset : offset + batch_size]
                try:
                    translated = _translate_batch(
                        translator,
                        [chunk.text for chunk in batch_refs],
                        source_language=source_language,
                    )
                except Exception as exc:
                    for chunk in batch_refs:
                        failed_tasks[chunk.task_id] = f"{type(exc).__name__}: {exc}"
                    continue

                for chunk, translated_text in zip(batch_refs, translated, strict=False):
                    translated_chunks[chunk.task_id][chunk.chunk_index] = translated_text

        for item in translate_tasks:
            detection = detections_by_task[item.task.task_id]
            elapsed = base._format_elapsed(time.perf_counter() - started_by_task[item.task.task_id])
            if item.task.task_id in failed_tasks:
                results.append(
                    common.LogTranslationResult(
                        task_id=item.task.task_id,
                        section_lines=[
                            f"{base.TRANSLATION_MARKER_PREFIX}{detection.display_name}: {elapsed}",
                            f"   [translation failed: {failed_tasks[item.task.task_id]}]",
                        ],
                        status="unavailable",
                    )
                )
                continue

            ordered_chunks = translated_chunks[item.task.task_id]
            translated_text = base._compact_text(
                " ".join(ordered_chunks[index] for index in sorted(ordered_chunks))
            )
            results.append(
                common.LogTranslationResult(
                    task_id=item.task.task_id,
                    section_lines=[
                        f"{base.TRANSLATION_MARKER_PREFIX}{detection.display_name}: {elapsed}",
                        *base._wrapped_section_lines(translated_text),
                    ],
                    status="translated",
                )
            )

    results.sort(key=lambda result: result.task_id)
    common.write_and_report(
        backend=f"batch(size={batch_size})",
        config=config,
        parsed=parsed,
        results=results,
        newline=newline,
        had_trailing_newline=had_trailing_newline,
    )


if __name__ == "__main__":
    main()
