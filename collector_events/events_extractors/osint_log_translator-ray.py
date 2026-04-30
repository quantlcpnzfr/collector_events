"""Ray OSINT log translator.

Runs Telegram message-block translation across Ray actors. Default actor count
is os.cpu_count(), configurable with --workers.
"""

from __future__ import annotations

import argparse
import logging
import os
import warnings

import ray

from collector_events.events_extractors import osint_log_translator as base
from collector_events.events_extractors import osint_log_translator_parallel_common as common


@ray.remote
class RayTranslationActor:
    def __init__(self, config_payload: dict) -> None:
        self.config = common.config_from_payload(config_payload)
        base._configure_transformers_output(quiet=self.config.quiet)

    def process(self, task_payload: dict) -> dict:
        task = common.task_from_payload(task_payload)
        result = common.process_message_task(task, self.config)
        return common.result_to_payload(result)


def parse_args() -> argparse.Namespace:
    base._load_dotenv_files()
    parser = argparse.ArgumentParser(description="Translate OSINT log blocks with Ray actors.")
    common.add_common_args(parser)
    parser.add_argument("--ray-address", default="", help="Optional Ray cluster address. Empty starts local Ray.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = common.config_from_args(args)
    base._configure_transformers_output(quiet=config.quiet)
    if config.quiet:
        os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
        warnings.filterwarnings("ignore", category=FutureWarning, module=r"ray\..*")

    _, newline, had_trailing_newline, parsed = common.read_parsed_log(config)
    if not parsed.tasks:
        common.write_and_report(
            backend="ray",
            config=config,
            parsed=parsed,
            results=[],
            newline=newline,
            had_trailing_newline=had_trailing_newline,
        )
        return

    init_kwargs = {
        "ignore_reinit_error": True,
        "include_dashboard": False,
        "log_to_driver": not config.quiet,
        "configure_logging": not config.quiet,
        "logging_level": logging.ERROR if config.quiet else logging.INFO,
    }
    if args.ray_address:
        init_kwargs["address"] = args.ray_address

    owns_ray = not ray.is_initialized()
    if owns_ray:
        ray.init(**init_kwargs)

    try:
        config_payload = common.config_to_payload(config)
        actors = [RayTranslationActor.remote(config_payload) for _ in range(config.workers)]
        refs = [
            actors[index % len(actors)].process.remote(common.task_to_payload(task))
            for index, task in enumerate(parsed.tasks)
        ]
        results = [common.result_from_payload(payload) for payload in ray.get(refs)]
    finally:
        if owns_ray:
            ray.shutdown()

    results.sort(key=lambda result: result.task_id)
    common.write_and_report(
        backend="ray",
        config=config,
        parsed=parsed,
        results=results,
        newline=newline,
        had_trailing_newline=had_trailing_newline,
    )


if __name__ == "__main__":
    main()
