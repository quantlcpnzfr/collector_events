from __future__ import annotations
import asyncio
from typing import Any, Dict, List, Optional, Mapping

from forex_shared.worker_api import (
    BaseSessionWorker, 
    InMemorySessionPoolMixin,
    CommandResponse,
    WorkerCommand,
    WorkerStatus
)
from forex_shared.logging.get_logger import get_logger

from .session import OsintTelegramSession
from .store import OsintTelegramStore

log = get_logger(__name__)

class OsintTelegramWorker(BaseSessionWorker, InMemorySessionPoolMixin):
    """
    Worker process for Telegram OSINT collection.
    Manages multiple OsintTelegramSession instances.
    """
    
    def __init__(self, worker_id: str = "osint-telegram-worker", max_sessions: int = 10):
        self.worker_id = worker_id
        self.max_sessions = max_sessions
        self.control_topic = f"worker.{worker_id}.control"
        self._sessions: Dict[str, OsintTelegramSession] = {}
        self.store: Optional[OsintTelegramStore] = None
        self._stop_event = asyncio.Event()

    @property
    def sessions(self) -> Dict[str, OsintTelegramSession]:
        return self._sessions

    async def start(self) -> None:
        """Startup the worker and resources."""
        await self.on_startup()
        log.info(f"OsintTelegramWorker '{self.worker_id}' started.")

    async def on_startup(self) -> None:
        """Initialize worker-wide resources like the store."""
        if self.store is None:
            self.store = OsintTelegramStore()
            await self.store.ensure_indexes()
        log.info(f"Worker resources (MongoDB store) initialized.")

    async def stop(self) -> None:
        """Stop the worker and all sessions."""
        self._stop_event.set()
        await self.on_shutdown()
        log.info(f"OsintTelegramWorker '{self.worker_id}' stopped.")

    async def on_shutdown(self) -> None:
        """Stop all active sessions."""
        log.info(f"Stopping {len(self._sessions)} active sessions...")
        stop_tasks = [s.stop() for s in self._sessions.values()]
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        self._sessions.clear()

    async def run_forever(self) -> None:
        """Keep the worker process alive."""
        log.info(f"Worker '{self.worker_id}' entering run_forever loop.")
        while not self._stop_event.is_set():
            await asyncio.sleep(1.0)

    async def handle_command(self, payload: Mapping[str, Any]) -> CommandResponse:
        """Directly handle a command (e.g. for testing)."""
        return await self.dispatch_command(payload)

    async def create_session(self, payload: Mapping[str, Any]) -> CommandResponse:
        """Create and start a new Telegram OSINT session."""
        session_id = str(payload.get("session_id", ""))
        config = payload.get("config", {})
        
        if not session_id:
            return {"status": "error", "message": "session_id is required"}
            
        if session_id in self._sessions:
            return {"status": "error", "message": f"Session '{session_id}' already exists"}

        if not self.has_capacity:
            return {"status": "error", "message": "Worker at maximum session capacity"}

        try:
            if self.store is None:
                await self.on_startup()
                
            session = OsintTelegramSession(config, self.store)
            session.session_id = session_id
            
            await session.start()
            self._sessions[session_id] = session
            
            log.info(f"Session '{session_id}' created and started.")
            return {"status": "success", "session_id": session_id}
        except Exception as e:
            log.exception(f"Failed to create session '{session_id}': {e}")
            return {"status": "error", "message": str(e)}

    async def stop_session(self, session_id: str) -> CommandResponse:
        """Stop and remove a session."""
        if session_id not in self._sessions:
            return {"status": "error", "message": f"Session '{session_id}' not found"}

        try:
            session = self._sessions.pop(session_id)
            await session.stop()
            log.info(f"Session '{session_id}' stopped and removed.")
            return {"status": "success", "session_id": session_id}
        except Exception as e:
            log.exception(f"Error stopping session '{session_id}': {e}")
            return {"status": "error", "message": str(e)}

    async def list_sessions(self) -> List[Dict[str, Any]]:
        """Return snapshots of all active sessions."""
        return self.session_snapshots()

    async def health_check(self) -> CommandResponse:
        """Return worker health status."""
        return self.snapshot().to_dict()

if __name__ == "__main__":
    import asyncio
    
    async def main():
        worker = OsintTelegramWorker()
        await worker.start()
        try:
            await worker.run_forever()
        except KeyboardInterrupt:
            await worker.stop()

    asyncio.run(main())
