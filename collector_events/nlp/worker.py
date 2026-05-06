from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Mapping, Optional

from forex_shared.logging.get_logger import get_logger
from forex_shared.worker_api import (
    BaseSessionWorker,
    CommandResponse,
    InMemorySessionPoolMixin,
)

from collector_events.processors.event_processor import EventProcessor

from .session import NLPEnrichmentSession

log = get_logger(__name__)


class NLPWorker(BaseSessionWorker, InMemorySessionPoolMixin):
    """
    Worker-api process for NLP enrichment over MQ-delivered IntelItem payloads.
    """

    def __init__(self, worker_id: str = "nlp-worker", max_sessions: int = 5):
        self.worker_id = worker_id
        self.max_sessions = max_sessions
        self.control_topic = f"worker.{worker_id}.control"
        self._sessions: Dict[str, NLPEnrichmentSession] = {}
        self._processor: Optional[EventProcessor] = None
        self._stop_event = asyncio.Event()

    @property
    def sessions(self) -> Dict[str, NLPEnrichmentSession]:
        return self._sessions

    async def start(self) -> None:
        await self.on_startup()
        log.info("NLPWorker '%s' started.", self.worker_id)

    async def on_startup(self) -> None:
        if self._processor is None:
            self._processor = EventProcessor()
        log.info("NLPWorker initialized shared EventProcessor.")

    async def stop(self) -> None:
        self._stop_event.set()
        await self.on_shutdown()
        log.info("NLPWorker '%s' stopped.", self.worker_id)

    async def on_shutdown(self) -> None:
        stop_tasks = [session.stop() for session in self._sessions.values()]
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        self._sessions.clear()

    async def run_forever(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(1.0)

    async def list_sessions(self) -> List[Dict[str, Any]]:
        return self.session_snapshots()

    async def create_session(self, payload: Mapping[str, Any]) -> CommandResponse:
        session_id = str(payload.get("session_id", "default"))
        config = dict(payload.get("config", {}))
        config.setdefault("session_id", session_id)

        if session_id in self._sessions:
            return {"status": "error", "message": f"Session '{session_id}' already exists"}

        if not self.has_capacity:
            return {"status": "error", "message": "Worker at maximum capacity"}

        try:
            if self._processor is None:
                await self.on_startup()

            session = NLPEnrichmentSession(config, processor=self._processor)
            await session.start()
            self._sessions[session_id] = session
            log.info("NLP session '%s' created.", session_id)
            return {"status": "success", "session_id": session_id}
        except Exception as exc:
            log.exception("Failed to create NLP session '%s': %s", session_id, exc)
            return {"status": "error", "message": str(exc)}

    async def stop_session(self, session_id: str) -> CommandResponse:
        if session_id not in self._sessions:
            return {"status": "error", "message": f"Session '{session_id}' not found"}

        try:
            session = self._sessions.pop(session_id)
            await session.stop()
            return {"status": "success", "session_id": session_id}
        except Exception as exc:
            log.exception("Error stopping NLP session '%s': %s", session_id, exc)
            return {"status": "error", "message": str(exc)}

    async def health_check(self) -> CommandResponse:
        return self.snapshot().to_dict()
