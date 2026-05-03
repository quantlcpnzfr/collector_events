from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Literal

from forex_shared.env_config_manager import EnvConfigManager

# Carrega .env/config antes de subir config/env do orquestrador.
EnvConfigManager.startup()

from collector_events.globalintel.base import CHROME_UA, DEFAULT_TIMEOUT, ExtractionResult
from collector_events.globalintel.extractor_factory import ScheduleEntry
from collector_events.globalintel.orchestrator import IntelOrchestrator
from collector_events.globalintel.test_extractor import TestExtractor
from collector_events.nlp.nlp_engine import LocalNLPEngine
from collector_events.processors.tag_emitter import GlobalTagEmitter

import aiohttp

LabelSetName = Literal["minimum", "medium", "full"]


class DummyMQ:
    """MQ fake para teste local. Não publica nada fora do processo."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def publish(self, queue_name: str, message: Any) -> None:
        self.messages.append(
            {
                "queue_name": queue_name,
                "message": message,
            }
        )


class TestOrchestrator(IntelOrchestrator):
    """
    Orquestrador de teste isolado para NLP + GlobalTag dry-run.

    - Força LocalNLPEngine com labels_set/modelo antes do EventProcessor.
    - Roda serial, sem APScheduler.
    - Não grava Redis nem publica RabbitMQ por padrão.
    - Grava JSON progressivo.
    - Simula GlobalTagEmitter em dry-run e injeta o relatório no JSON.
    """

    def __init__(
        self,
        input_filepath: str | Path,
        output_filepath: str | Path,
        interval_seconds: float = 1.0,
        *,
        labels_set: LabelSetName = "minimum",
        persist_to_redis: bool = False,
        checkpoint_each_item: bool = True,
        enable_global_tag_dry_run: bool = True,
    ) -> None:
        LocalNLPEngine.get_instance(
            labels_set=labels_set,
            zero_shot_model="cross-encoder/nli-deberta-v3-small",
            use_fast_tokenizer=False,
            local_files_only=False,
        )

        super().__init__()

        self.input_filepath = Path(input_filepath)
        self.output_filepath = Path(output_filepath)
        self.interval_seconds = float(interval_seconds)
        self.labels_set = labels_set
        self.persist_to_redis = persist_to_redis
        self.checkpoint_each_item = checkpoint_each_item
        self.enable_global_tag_dry_run = enable_global_tag_dry_run

        self.enriched_results: list[dict[str, Any]] = []
        self._write_lock = asyncio.Lock()

        self.test_extractor = TestExtractor(filepath=self.input_filepath)
        self._test_entry = ScheduleEntry(
            extractor=self.test_extractor,
            interval_seconds=int(self.interval_seconds),
        )
        self._schedule = [self._test_entry]
        self.total_items = len(self.test_extractor.items_data)

        self._dummy_mq = DummyMQ()
        if self.enable_global_tag_dry_run:
            self._tag_emitter = GlobalTagEmitter(
                mq_provider=self._dummy_mq,
                dry_run=True,
            )

    async def _enrich_and_store(self, result: ExtractionResult, extractor) -> None:
        if self.persist_to_redis:
            await super()._enrich_and_store(result, extractor)
        else:
            await self._enrich_items_without_external_side_effects(result)

        if not result.items:
            return

        for item in result.items:
            self.enriched_results.append(self._safe_to_dict(item))

        self.log.info(
            "💾 Interceptado em memória: %s/%s itens | último lote=%s",
            len(self.enriched_results),
            self.total_items,
            len(result.items),
        )

        if self.checkpoint_each_item:
            await self._write_json_atomic(
                self.output_filepath,
                self.enriched_results,
            )
            self.log.info(
                "📝 Checkpoint progressivo gravado em: %s | itens=%s/%s",
                self.output_filepath,
                len(self.enriched_results),
                self.total_items,
            )

    async def _enrich_items_without_external_side_effects(self, result: ExtractionResult) -> None:
        if not result.items:
            return

        for item in result.items:
            try:
                if item.extra is None:
                    item.extra = {}

                processed = await asyncio.to_thread(self._processor.process_item, item)

                item.extra["danger_score"] = processed.danger_score
                item.extra["impact_category"] = processed.impact_category

                if processed.features:
                    item.extra["gliner_tactical"] = processed.features.get(
                        "gliner_graph",
                        {},
                    )

                if self.enable_global_tag_dry_run and self._tag_emitter:
                    emission_report = await self._tag_emitter.emit_if_critical(item)
                    item.extra["global_tag_emission"] = emission_report

                    if emission_report.get("emitted"):
                        self.log.info(
                            "🏷️ TEST GlobalTag dry-run emitida | event=%s | count=%s | assets=%s",
                            getattr(item, "id", "<sem-id>"),
                            emission_report.get("emitted_count"),
                            [tag.get("asset") for tag in emission_report.get("tags", [])],
                        )
                    else:
                        self.log.info(
                            "🏷️ TEST GlobalTag não emitida | event=%s | reason=%s | score=%.3f",
                            getattr(item, "id", "<sem-id>"),
                            emission_report.get("reason"),
                            float(emission_report.get("danger_score", 0.0)),
                        )

            except Exception as exc:
                item_id = getattr(item, "id", "<sem-id>")
                self.log.exception("Falha na IA para o item %s: %s", item_id, exc)

    async def run_serial_test(self) -> None:
        if self.total_items == 0:
            self.log.error("Nenhum item para processar. Arquivo existe? %s", self.input_filepath)
            return

        self.log.info(
            "🚀 Teste NLP serial iniciado | itens=%s | intervalo=%.2fs | labels_set=%s | redis=%s | tag_dry_run=%s",
            self.total_items,
            self.interval_seconds,
            self.labels_set,
            self.persist_to_redis,
            self.enable_global_tag_dry_run,
        )

        async with aiohttp.ClientSession(
            timeout=DEFAULT_TIMEOUT,
            headers={"User-Agent": CHROME_UA},
        ) as session:
            while self.test_extractor.current_index < self.total_items:
                before = self.test_extractor.current_index
                result = await self._run_entry(self._test_entry, session=session)
                after = self.test_extractor.current_index

                if result.error:
                    self.log.error("Erro no resultado do extrator: %s", result.error)

                if after == before:
                    self.log.warning(
                        "Extractor não avançou índice (%s). Encerrando para evitar loop infinito.",
                        before,
                    )
                    break

                if after < self.total_items and self.interval_seconds > 0:
                    await asyncio.sleep(self.interval_seconds)

        await self._write_json_atomic(self.output_filepath, self.enriched_results)
        self.log.info(
            "✅ Teste concluído. Arquivo final consolidado com %s itens em %s",
            len(self.enriched_results),
            self.output_filepath,
        )

        await self._cleanup_connections()

    async def _cleanup_connections(self) -> None:
        try:
            await self.disconnect_mq()
        except Exception as exc:
            self.log.warning("Falha ao desconectar MQ no cleanup: %s", exc)

        try:
            await self.disconnect_redis()
        except Exception as exc:
            self.log.warning("Falha ao desconectar Redis no cleanup: %s", exc)

    async def _write_json_atomic(self, filepath: Path, payload: Any) -> None:
        async with self._write_lock:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            temp_filepath = filepath.with_suffix(filepath.suffix + ".tmp")

            with open(temp_filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4, default=str)
                f.write("\n")

            os.replace(temp_filepath, filepath)

    def _safe_to_dict(self, item: Any) -> dict[str, Any]:
        if is_dataclass(item):
            return asdict(item)

        if hasattr(item, "model_dump"):
            return item.model_dump(mode="json")

        if hasattr(item, "dict"):
            return item.dict()

        if hasattr(item, "__dict__"):
            return dict(item.__dict__)

        return {"value": str(item)}


async def run_test_pipeline() -> None:
    current_dir = Path(__file__).parent

    INPUT_JSON = current_dir / "mock_intel_items_big.json"
    OUTPUT_JSON = current_dir / "mock_intel_items_big_process_result.json"

    INTERVALO_SEGUNDOS = 5
    LABELS_SET: LabelSetName = "minimum"
    PERSIST_TO_REDIS = False
    CHECKPOINT_EACH_ITEM = True
    ENABLE_GLOBAL_TAG_DRY_RUN = True

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print(
        "\n--- Iniciando Pipeline de NLP Isolada "
        f"| intervalo={INTERVALO_SEGUNDOS}s "
        f"| labels_set={LABELS_SET} "
        f"| redis={PERSIST_TO_REDIS} "
        f"| checkpoint={CHECKPOINT_EACH_ITEM} "
        f"| tag_dry_run={ENABLE_GLOBAL_TAG_DRY_RUN}\n"
    )

    orch = TestOrchestrator(
        input_filepath=INPUT_JSON,
        output_filepath=OUTPUT_JSON,
        interval_seconds=INTERVALO_SEGUNDOS,
        labels_set=LABELS_SET,
        persist_to_redis=PERSIST_TO_REDIS,
        checkpoint_each_item=CHECKPOINT_EACH_ITEM,
        enable_global_tag_dry_run=ENABLE_GLOBAL_TAG_DRY_RUN,
    )

    await orch.run_serial_test()

    print(f"\n--- Teste concluido. Verifique o arquivo: {OUTPUT_JSON}")


if __name__ == "__main__":
    asyncio.run(run_test_pipeline())
