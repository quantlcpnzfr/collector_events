from datetime import datetime, timezone
from typing import Any, Dict, Optional
from forex_shared.worker_api import BaseStore
from forex_shared.mongo_manager import MongoManager

class OsintTelegramStore(BaseStore):
    """MongoDB store for Telegram sessions and cursors."""
    
    def __init__(self, mongo_manager: Optional[MongoManager] = None):
        super().__init__(mongo_manager)
        self.cursor_collection = "telegram_osint"
        self.session_collection = "telegram"

    async def ensure_indexes(self) -> None:
        """Create indexes for performance and uniqueness."""
        # Index for cursors
        await self._mongo.async_ensure_indexes(
            self.cursor_collection, 
            [[("key", 1)]]
        )
        # Index for sessions
        await self._mongo.async_ensure_indexes(
            self.session_collection,
            [[("_id", 1)]]
        )

    async def store_item(self, collection: str, item: Any, filter_: Dict[str, Any]) -> Any:
        """Helper to persist items with custom filter."""
        return await self._mongo.async_replace_one(
            collection,
            filter_,
            item,
            upsert=True
        )

    async def get_item(self, collection: str, query: Dict[str, Any]) -> Optional[Any]:
        """Retrieve an item by query."""
        results = await self._mongo.async_find_many(
            collection,
            filter_=query,
            limit=1
        )
        return results[0] if results else None

    async def get_session(self, session_id: str) -> Optional[str]:
        """Load StringSession from 'telegram' collection using _id."""
        doc = await self.get_item(self.session_collection, {"_id": session_id})
        return doc.get("session_string") if doc else None

    async def save_session(self, session_id: str, session_string: str) -> None:
        """Save StringSession to 'telegram' collection using _id."""
        await self.store_item(
            self.session_collection,
            {
                "_id": session_id,
                "session_string": session_string,
                "updated_at": datetime.now(timezone.utc)
            },
            {"_id": session_id}
        )

    async def get_cursor(self, handle: str) -> int:
        """Load last processed message ID for a handle from 'telegram_osint'."""
        doc = await self.get_item(self.cursor_collection, {"key": f"cursor:{handle.lower()}"})
        return doc.get("last_id", 0) if doc else 0

    async def save_cursor(self, handle: str, last_id: int) -> None:
        """Save last processed message ID for a handle to 'telegram_osint'."""
        await self.store_item(
            self.cursor_collection,
            {
                "key": f"cursor:{handle.lower()}",
                "last_id": last_id,
                "updated_at": datetime.now(timezone.utc)
            },
            {"key": f"cursor:{handle.lower()}"}
        )
