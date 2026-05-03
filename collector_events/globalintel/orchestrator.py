"""Global intelligence orchestrator.

Runs all extractors on a schedule, stores results via IntelCache (Redis),
and provides a unified interface for the forex_system pipeline.

Usage:
    from collector_events.globalintel.orchestrator import IntelOrchestrator

    orch = IntelOrchestrator()                # uses GlobalIntelConfig from env
    await orch.run_all()                      # one-shot all extractors (shared session)
    await orch.run_domain("cyber")            # single domain (shared session)
    await orch.start_scheduler()              # background APScheduler loop
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from collector_events.globalintel.base import BaseExtractor, ExtractionResult, CHROME_UA, DEFAULT_TIMEOUT
from collector_events.globalintel.cache import IntelCache
from collector_events.globalintel.config_env import GlobalIntelConfig
from collector_events.globalintel.extractor_factory import (
    ExtractorFactory,
    OrchestratorConfig,
    ScheduleEntry,
)
from collector_events.globalintel.global_tag_manager import GlobalTagManager
from collector_events.processors.tag_emitter import GlobalTagEmitter
from collector_events.processors import EventProcessor

from forex_shared.logging.get_logger import get_logger
from forex_shared.logging.loggable import Loggable
from forex_shared.providers.cache.redis_provider import RedisProvider
from forex_shared.providers.mq.mq_factory import MQFactory
from forex_shared.providers.mq.mq_provider_async import MQProviderAsync
from forex_shared.providers.mq.topics import IntelTopics

logger = get_logger(__name__)


class IntelOrchestrator(Loggable):
    """Central orchestrator for all global intelligence extractors.

    Instantiates all extractors via ``ExtractorFactory``, runs them on a
    schedule via APScheduler, and stores results to Redis via IntelCache.

    Uses ``GlobalIntelConfig`` (hot-reloadable via ``EnvConfigManager``) by
    default.  Accepts a legacy ``OrchestratorConfig`` for backward compatibility.
    """

    def __init__(
        self,
        config: GlobalIntelConfig | OrchestratorConfig | None = None,
        mq: MQProviderAsync | None = None,
        sources_filter: set[str] | None = None,
        on_result: Callable[[ExtractionResult], Awaitable[None]] | None = None,
    ) -> None:
        if config is None:
            config = GlobalIntelConfig() # type: ignore
        self._config = config
        self._mq: MQProviderAsync | None = mq
        self._redis: RedisProvider | None = None
        self._cache: IntelCache | None = None
        self._tag_manager: GlobalTagManager | None = None
        self._tag_emitter: GlobalTagEmitter | None = None
        
        # Instancia o scheduler APScheduler para rodar 
        # os extratores em background
        self._scheduler: AsyncIOScheduler = AsyncIOScheduler()
        
        # Inicia o motor de IA que faz uso do NLP Engine para processar 
        # os resultados e gerar insights adicionais
        self._processor = EventProcessor() 
        
        self._sources_filter = sources_filter
        
        # Callback para processar resultados, como a persistência MongoDB
        self._on_result = on_result

        factory = ExtractorFactory(config)
        self._schedule: list[ScheduleEntry] = factory.build_schedule(
            sources=sources_filter,
        )

    # ── Config helpers ───────────────────────────────────────────────

    def _get_redis_url(self) -> str:
        if isinstance(self._config, GlobalIntelConfig):
            return self._config.REDIS_URL
        return self._config.redis_url # type: ignore

    # ── MQ helpers ───────────────────────────────────────────────────

    async def connect_mq(self, mq: MQProviderAsync | None = None) -> None:
        """Connect MQ provider.  If ``mq`` is provided, uses it directly.

        Call this before ``start_scheduler()`` to enable MQ publishing.
        If not called, the orchestrator runs in cache-only mode (no MQ).
        """
        if mq is not None:
            self._mq = mq
        if self._mq is None:
            self._mq = MQFactory.create_async_from_env()
        await self._mq.connect()
        
        redis_provider = await self._get_redis_provider()
        
        # Inicializa MQ apenas se houver configuração
        if self._mq:
            self._tag_emitter = GlobalTagEmitter(mq_provider=self._mq)
        else:
            self.log.warning("RabbitMQ não connectado. TagEmitter operando em modo SILENT.")
            self._tag_emitter = None
        self.log.info("GlobalTagEmitter conectado e pronto.")

        if self._mq:
            self._tag_manager = GlobalTagManager(redis_provider=redis_provider, mq=self._mq)
        else:
            self.log.warning("RabbitMQ não connectado. TagManager operando em modo SILENT.")
            self._tag_manager = None
        
        self.log.info("IntelOrchestrator MQ connected; GlobalTagManager and GlobalTagEmitter ready.")

    async def disconnect_mq(self) -> None:
        """Gracefully disconnect MQ provider."""
        if self._mq is not None:
            await self._mq.disconnect()
            self._mq = None
            self._tag_manager = None
            self.log.info("IntelOrchestrator MQ disconnected")

    async def disconnect_redis(self) -> None:
        """Gracefully disconnect Redis provider."""
        if self._redis is not None:
            await self._redis.disconnect()
            self._redis = None
            self._cache = None
            self.log.info("IntelOrchestrator Redis disconnected")

    async def _get_redis_provider(self) -> RedisProvider:
        """Return a connected RedisProvider singleton."""
        try:
            self._redis = await RedisProvider.shared_from_env()
            return self._redis
        except Exception:
            self.log.exception("Failed to connect to Redis")
            raise


    async def _get_cache(self) -> IntelCache:
        redis_provider = await self._get_redis_provider()

        if self._cache is None or self._cache._r is not redis_provider:
            self._cache = IntelCache(redis_provider)

        return self._cache
    
    async def _enrich_and_store(self, result: ExtractionResult, extractor: BaseExtractor) -> None:
        """
        Processa os itens com os modelos SLM (CPU-bound) em threads separadas
        e salva o resultado final no Redis.
        """
        if result.items:
            for item in result.items:
                try:
                    
                    # 1. IA Processa (NLP) para extrair features, inferir categorias 
                    # e calcular o score de risco:
                    # Envia a carga pesada de NLP para uma thread, mantendo o asyncio livre
                    processed = await asyncio.to_thread(self._processor.process_item, item)
                    
                    # 2. Injeta resultado do Item: 
                    # Injeta o Score e Categorias no item antes de ir para o banco
                    item.extra["danger_score"] = processed.danger_score
                    item.extra["impact_category"] = processed.impact_category
                    
                    # ⚡ 3. DADOS TÁTICOS DO GLiNER PARA ALGORITMOS AVANÇADOS
                    if processed.features:
                        item.extra["gliner_tactical"] = processed.features.get("gliner_graph", {})
                    
                    # ⚡ 4. GATILHO DE GLOBAL TAG (Se o score for crítico, gera o sinal de trading)
                    if self._tag_emitter:
                        await self._tag_emitter.emit_if_critical(item)
                    else:
                        self.log.warning("TagEmitter não inicializado. Ignorando emissão de tags.")
                        
                except Exception as e:
                    self.log.error("Falha na IA para o item %s: %s", item.id, e)

        # Agora que os dados estão enriquecidos, salvamos no Cache
        cache = await self._get_cache()
        await cache.store(result, key=extractor.REDIS_KEY, ttl=extractor.TTL_SECONDS)

    # ── One-shot methods ─────────────────────────────────────────────

    async def run_all(self) -> list[ExtractionResult]:
        """Run all extractors once with a shared aiohttp session."""
        results: list[ExtractionResult] = []
        async with aiohttp.ClientSession(
            timeout=DEFAULT_TIMEOUT,
            headers={"User-Agent": CHROME_UA},
        ) as session:
            for entry in self._schedule:
                result = await self._run_entry(entry, session=session)
                results.append(result)
        return results

    async def run_domain(self, domain: str) -> list[ExtractionResult]:
        """Run extractors for a single domain with a shared session."""
        results: list[ExtractionResult] = []
        async with aiohttp.ClientSession(
            timeout=DEFAULT_TIMEOUT,
            headers={"User-Agent": CHROME_UA},
        ) as session:
            for entry in self._schedule:
                if entry.extractor.DOMAIN == domain:
                    result = await self._run_entry(entry, session=session)
                    results.append(result)
        return results

    async def run_source(self, source: str) -> ExtractionResult | None:
        """Run a single extractor by SOURCE name."""
        for entry in self._schedule:
            if entry.extractor.SOURCE == source:
                return await self._run_entry(entry)
        return None

    async def _run_entry(
            self, 
            entry: ScheduleEntry, 
            session: aiohttp.ClientSession | None = None) -> ExtractionResult:
        """
        Executa a extração para uma única entry, aplica IA e salva no Redis.
        Centraliza a lógica para run_all, run_domain e start_scheduler.
        """
        extractor = entry.extractor
        own_session = False
        
        if session is None:
            session = aiohttp.ClientSession(timeout=DEFAULT_TIMEOUT, headers={"User-Agent": CHROME_UA})
            own_session = True

        try:
            # 1. NETWORK I/O (Baixa os dados crus)
            result = await extractor.run(session)

            # 2. IA / ENRIQUECIMENTO (Aplica NLP sem bloquear o event loop) e 3. REDIS I/O
            await self._enrich_and_store(result, extractor)

            # 4. PERSISTÊNCIA (MongoDB via on_result callback, ex: IntelMongoStore.store_result)
            if self._on_result is not None:
                try:
                    await self._on_result(result)
                except Exception as cb_exc:
                    self.log.error(
                        "on_result callback falhou para %s: %s",
                        extractor.SOURCE, cb_exc,
                    )

            # Atualiza o timestamp da última execução
            entry.last_run = datetime.now(timezone.utc).timestamp()
            return result
            
        except Exception as e:
            self.log.error("Falha ao executar %s: %s", extractor.SOURCE, e)
            # Retorna um resultado vazio em caso de falha de rede
            return ExtractionResult(source=extractor.SOURCE, domain=extractor.DOMAIN, error=str(e))
        finally:
            if own_session:
                try: 
                    await session.close()
                except (asyncio.CancelledError, Exception):
                    pass

    # ── Scheduler loop ───────────────────────────────────────────────

    async def start_scheduler(self) -> None:
        """Start the APScheduler background loop — one job per extractor."""
        now = datetime.now(timezone.utc)
        
        try: 
            if hasattr(self, 'connect_mq'):
                await self.connect_mq()
        except Exception as e:
            self.log.error(f"Erro ao conectar MQ: {e}. Continuando sem MQ.")

        for index, entry in enumerate(self._schedule):
            self._scheduler.add_job(
                self._run_entry,
                trigger="interval",
                seconds=entry.interval_seconds,
                args=[entry],
                id=f"{entry.extractor.DOMAIN}_{entry.extractor.SOURCE}",
                next_run_time=now + timedelta(seconds=index * 5),
                misfire_grace_time=60,
                coalesce=True,
                max_instances=1,
            )

        self._scheduler.start()
        self.log.info(
            "IntelOrchestrator scheduler started with %d extractors",
            len(self._schedule),
        )

    def stop_scheduler(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    # ── Status / health ──────────────────────────────────────────────

    async def health(self) -> dict[str, Any]:
        """Return health status of all extractors (async — reads Redis)."""
        cache = await self._get_cache()
        status: dict[str, Any] = {}
        for entry in self._schedule:
            ext = entry.extractor
            key = f"{ext.DOMAIN}/{ext.SOURCE}"
            meta = await cache.health(ext.REDIS_KEY)
            status[key] = {
                "redis_key": ext.REDIS_KEY,
                "ttl_seconds": ext.TTL_SECONDS,
                "interval_seconds": entry.interval_seconds,
                "last_run": entry.last_run,
                "cache_meta": meta,
            }
        return status

    @property
    def extractors(self) -> list[BaseExtractor]:
        return [e.extractor for e in self._schedule]

    @property
    def domains(self) -> set[str]:
        return {e.extractor.DOMAIN for e in self._schedule}
