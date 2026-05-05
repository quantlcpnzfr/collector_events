from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Mapping

from forex_shared.worker_api import (
    BaseSessionWorker, 
    InMemorySessionPoolMixin,
    CommandResponse
)
from forex_shared.logging.get_logger import get_logger

from .session import TranslationSession
from .engine import TranslationEngine

log = get_logger(__name__)

class TranslationWorker(BaseSessionWorker, InMemorySessionPoolMixin):
    """
    Worker process for OSINT translation.
    Manages multiple TranslationSession instances.
    """
    
    def __init__(self, worker_id: str = "translation-worker", max_sessions: int = 5):
        self.worker_id = worker_id
        self.max_sessions = max_sessions
        self.control_topic = f"worker.{worker_id}.control"
        self._sessions: Dict[str, TranslationSession] = {}
        self._engine: Optional[TranslationEngine] = None
        self._stop_event = asyncio.Event()

    @property
    def sessions(self) -> Dict[str, TranslationSession]:
        return self._sessions

    async def start(self) -> None:
        """Startup the worker."""
        await self.on_startup()
        log.info(f"TranslationWorker '{self.worker_id}' started.")

    async def on_startup(self) -> None:
        """Initialize worker-wide resources."""
        # We could initialize a shared engine here if all sessions use the same config
        log.info(f"TranslationWorker initializing...")

    async def stop(self) -> None:
        """Stop the worker and all sessions."""
        self._stop_event.set()
        await self.on_shutdown()
        log.info(f"TranslationWorker '{self.worker_id}' stopped.")

    async def on_shutdown(self) -> None:
        """Stop all active sessions."""
        log.info(f"Stopping {len(self._sessions)} active translation sessions...")
        stop_tasks = [s.stop() for s in self._sessions.values()]
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        self._sessions.clear()

    async def run_forever(self) -> None:
        """Keep the worker process alive."""
        while not self._stop_event.is_set():
            await asyncio.sleep(1.0)

    async def create_session(self, payload: Mapping[str, Any]) -> CommandResponse:
        """Create and start a new Translation session."""
        session_id = str(payload.get("session_id", "default"))
        config = payload.get("config", {})
        
        if session_id in self._sessions:
            return {"status": "error", "message": f"Session '{session_id}' already exists"}

        if not self.has_capacity:
            return {"status": "error", "message": "Worker at maximum capacity"}

        try:
            # Each session currently gets its own engine for simplicity, 
            # but we could share it if the config matches.
            session = TranslationSession(config)
            session.session_id = session_id
            
            await session.start()
            self._sessions[session_id] = session
            
            log.info(f"Translation session '{session_id}' created.")
            return {"status": "success", "session_id": session_id}
        except Exception as e:
            log.exception(f"Failed to create translation session '{session_id}': {e}")
            return {"status": "error", "message": str(e)}

    async def stop_session(self, session_id: str) -> CommandResponse:
        """Stop and remove a session."""
        if session_id not in self._sessions:
            return {"status": "error", "message": f"Session '{session_id}' not found"}

        try:
            session = self._sessions.pop(session_id)
            await session.stop()
            return {"status": "success", "session_id": session_id}
        except Exception as e:
            log.exception(f"Error stopping session '{session_id}': {e}")
            return {"status": "error", "message": str(e)}

    async def health_check(self) -> CommandResponse:
        """Return worker health status."""
        return self.snapshot().to_dict()

if __name__ == "__main__":
    import sys
    from forex_shared.env_config_manager import EnvConfigManager
    
    async def main():
        # Load env config
        EnvConfigManager.startup()
        
        worker_id = sys.argv[1] if len(sys.argv) > 1 else "translation-worker-01"
        worker = TranslationWorker(worker_id=worker_id)
        await worker.start()
        
        # In a real scenario, sessions are created via commands.
        # For a standalone run, we can auto-create a default session if configured.
        if os.getenv("AUTO_START_SESSION", "false").lower() == "true":
            await worker.create_session({"session_id": "default", "config": {}})
            
        try:
            await worker.run_forever()
        except KeyboardInterrupt:
            await worker.stop()

    asyncio.run(main())
