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

import aiohttp

LabelSetName = Literal["minimum", "medium", "full"]


class TestOrchestrator(IntelOrchestrator):
    """
    Orquestrador de teste isolado para NLP.

    Diferenças em relação ao fluxo normal:
    - força LocalNLPEngine com labels_set="minimum" antes do EventProcessor ser criado;
    - troca o schedule real por apenas um TestExtractor;
    - roda em modo serial, sem APScheduler, para evitar overlap/max_instances;
    - por padrão não publica MQ nem grava Redis, mantendo o teste focado em NLP;
    - mantém os resultados em memória e grava JSON final de forma atômica.
    """

    def __init__(
        self,
        input_filepath: str | Path,
        output_filepath: str | Path,
        interval_seconds: float = 1.0,
        *,
        labels_set: LabelSetName = "minimum",
        persist_to_redis: bool = False,
        checkpoint_each_item: bool = False,
    ) -> None:
        # IMPORTANTE:
        # IntelOrchestrator.__init__ cria EventProcessor(), e EventProcessor.__init__ chama
        # LocalNLPEngine.get_instance(). Então o singleton precisa nascer aqui, antes do super().
        LocalNLPEngine.get_instance(labels_set=labels_set)

        super().__init__()

        self.input_filepath = Path(input_filepath)
        self.output_filepath = Path(output_filepath)
        self.partial_output_filepath = self.output_filepath.with_suffix(
            self.output_filepath.suffix + ".partial"
        )
        self.interval_seconds = float(interval_seconds)
        self.labels_set = labels_set
        self.persist_to_redis = persist_to_redis
        self.checkpoint_each_item = checkpoint_each_item

        self.enriched_results: list[dict[str, Any]] = []
        self._write_lock = asyncio.Lock()

        # Substitui o schedule real por apenas o extrator de teste.
        self.test_extractor = TestExtractor(filepath=self.input_filepath)
        self._test_entry = ScheduleEntry(
            extractor=self.test_extractor,
            interval_seconds=int(self.interval_seconds),
        )
        self._schedule = [self._test_entry]
        self.total_items = len(self.test_extractor.items_data)

    async def _enrich_and_store(self, result: ExtractionResult, extractor) -> None:
        """
        Intercepta o enriquecimento para teste.

        Default: não chama super(), porque o super salva Redis e pode disparar caminho MQ.
        Para um teste de latência de NLP, isso é ruído.

        Se persist_to_redis=True, usa o fluxo do orquestrador pai e depois captura os itens.
        """
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
                self.partial_output_filepath,
                self.enriched_results,
            )
            self.log.info("📝 Checkpoint parcial gravado em: %s", self.partial_output_filepath)

    async def _enrich_items_without_external_side_effects(self, result: ExtractionResult) -> None:
        """
        Replica só a parte útil do IntelOrchestrator._enrich_and_store para teste:
        roda EventProcessor em thread e injeta danger_score/impact_category/GLiNER no item.

        Não grava Redis.
        Não publica RabbitMQ.
        Não chama TagEmitter.
        """
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

            except Exception as exc:
                item_id = getattr(item, "id", "<sem-id>")
                self.log.exception("Falha na IA para o item %s: %s", item_id, exc)

    async def run_serial_test(self) -> None:
        """
        Executa o TestExtractor item a item, de forma determinística.

        Essa função substitui o APScheduler no teste justamente para evitar:
        "maximum number of running instances reached" / "only one instance...".
        """
        if self.total_items == 0:
            self.log.error("Nenhum item para processar. Arquivo existe? %s", self.input_filepath)
            return

        self.log.info(
            "🚀 Teste NLP serial iniciado | itens=%s | intervalo=%.2fs | labels_set=%s | redis=%s",
            self.total_items,
            self.interval_seconds,
            self.labels_set,
            self.persist_to_redis,
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
            "✅ Teste concluído. Gravados %s itens em %s",
            len(self.enriched_results),
            self.output_filepath,
        )

        await self._cleanup_connections()

    async def _cleanup_connections(self) -> None:
        """Fecha conexões caso algum caminho opcional tenha aberto Redis/MQ."""
        try:
            await self.disconnect_mq()
        except Exception as exc:
            self.log.warning("Falha ao desconectar MQ no cleanup: %s", exc)

        try:
            await self.disconnect_redis()
        except Exception as exc:
            self.log.warning("Falha ao desconectar Redis no cleanup: %s", exc)

    async def _write_json_atomic(self, filepath: Path, payload: Any) -> None:
        """
        Escrita atômica e protegida por lock.

        Mesmo que no fluxo serial não exista concorrência real, isso deixa seguro caso
        você ligue checkpoint ou reaproveite a classe com tarefas concorrentes depois.
        """
        async with self._write_lock:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            temp_filepath = filepath.with_suffix(filepath.suffix + ".tmp")

            with open(temp_filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4, default=str)
                f.write("\n")

            os.replace(temp_filepath, filepath)

    def _safe_to_dict(self, item: Any) -> dict[str, Any]:
        """Converte dataclass/model simples para dict serializável."""
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

    INTERVALO_SEGUNDOS = 8
    LABELS_SET: LabelSetName = "minimum"

    # Para teste puro de NLP, deixe False.
    # Se quiser manter comportamento antigo de gravar Redis, mude para True.
    PERSIST_TO_REDIS = False

    # False = só grava JSON final, melhor para não ter disputa/IO pesado.
    # True = também grava .partial atomicamente a cada item.
    CHECKPOINT_EACH_ITEM = False

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print(
        "\n🚀 Iniciando Pipeline de NLP Isolada "
        f"| intervalo={INTERVALO_SEGUNDOS}s "
        f"| labels_set={LABELS_SET} "
        f"| redis={PERSIST_TO_REDIS}\n"
    )

    orch = TestOrchestrator(
        input_filepath=INPUT_JSON,
        output_filepath=OUTPUT_JSON,
        interval_seconds=INTERVALO_SEGUNDOS,
        labels_set=LABELS_SET,
        persist_to_redis=PERSIST_TO_REDIS,
        checkpoint_each_item=CHECKPOINT_EACH_ITEM,
    )

    await orch.run_serial_test()

    print(f"\n✅ Teste concluído. Verifique o arquivo: {OUTPUT_JSON}")


if __name__ == "__main__":
    asyncio.run(run_test_pipeline())
