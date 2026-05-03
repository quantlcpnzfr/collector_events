"""ThreadPool OSINT log translator.

Runs one translation task per Telegram message block using ThreadPoolExecutor.
Default worker count is os.cpu_count(), configurable with --workers.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from collector_events.events_extractors import osint_log_translator as base
from collector_events.events_extractors import osint_log_translator_parallel_common as common


def parse_args() -> argparse.Namespace:
    base._load_dotenv_files()
    parser = argparse.ArgumentParser(description="Translate OSINT log blocks with ThreadPoolExecutor.")
    common.add_common_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = common.config_from_args(args)
    base._configure_transformers_output(quiet=config.quiet)

    _, newline, had_trailing_newline, parsed = common.read_parsed_log(config)
    if not parsed.tasks:
        common.write_and_report(
            backend="threadpool",
            config=config,
            parsed=parsed,
            results=[],
            newline=newline,
            had_trailing_newline=had_trailing_newline,
        )
        return

    results: list[common.LogTranslationResult] = []
    with ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="osint-translate") as executor:
        futures = {
            executor.submit(common.process_message_task, task, config): task.task_id
            for task in parsed.tasks
        }
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda result: result.task_id)
    common.write_and_report(
        backend="threadpool",
        config=config,
        parsed=parsed,
        results=results,
        newline=newline,
        had_trailing_newline=had_trailing_newline,
    )


if __name__ == "__main__":
    main()
