import asyncio
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch

from collector_events.translation.engine import TranslationEngine
from collector_events.translation.session import TranslationSession

class TestMockTranslation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = Path(r"C:\Projects\forex_system\services\collector_events\collector_events\translation\tests")
        self.source_json = self.test_dir / "osint_feed_intel.json"
        self.output_json = self.test_dir / "osint_feed_translated.json"
        
        # Mock Config
        self.config = {
            "session_id": "test-session",
            "detector_model_path": ".models/translation/glotlid/model.bin",
            "translation_provider": os.getenv("TRANSLATION_TEST_PROVIDER", "opus_mt"),
            "translation_model_path": os.getenv(
                "TRANSLATION_TEST_MODEL",
                "Helsinki-NLP/opus-mt-mul-en",
            ),
            "device": os.getenv("TRANSLATION_TEST_DEVICE", "cpu"),
            "min_detection_confidence": 0.1, # Low for testing
        }
        
        # Mock MQ
        self.mock_mq = AsyncMock()
        
    @patch("forex_shared.providers.mq.mq_factory.MQFactory.create_async_from_env")
    async def test_enrichment_and_logging(self, mock_mq_factory):
        mock_mq_factory.return_value = self.mock_mq
        
        # 1. Read source JSON
        if not self.source_json.exists():
            self.skipTest(f"Source file {self.source_json} not found")
            
        with open(self.source_json, "r", encoding="utf-8") as f:
            items = json.load(f)
            
        # 2. Setup Session
        engine = TranslationEngine(self.config)
        engine.load()
        
        # Mock detection only because the GlotLID detector model is not present locally.
        # Translation itself must go through the real engine.
        original_detect = engine.detect_language
        def mock_detect(text):
            if any('\u0600' <= c <= '\u06FF' for c in text):
                return MagicMock(language="pes_Arab", language_name="Persian", confidence=0.9, status="detected", error="")
            if any('\u0590' <= c <= '\u05FF' for c in text):
                return MagicMock(language="heb_Hebr", language_name="Hebrew", confidence=0.9, status="detected", error="")
            if any('\u0400' <= c <= '\u04FF' for c in text):
                return MagicMock(language="ukr_Cyrl", language_name="Ukrainian", confidence=0.9, status="detected", error="")
            return original_detect(text)
        
        engine.detect_language = MagicMock(side_effect=mock_detect)

        session = TranslationSession(self.config, engine=engine)
        session._mq = self.mock_mq
        
        translated_items = []
        
        # 3. & 4. Process items and log results
        for item in items:
            # Simulate MQ receiving
            result = await asyncio.to_thread(session._process_payload, item)
            if result:
                # Check enrichment contract
                self.assertIn("translation_source", result["extra"])
                self.assertIn("original_body", result["extra"])
                self.assertNotIn("event_type", result)
                self.assertNotIn("translation", result)
                self.assertNotIn("source_language", result)
                self.assertNotIn("source_language_name", result)
                
                translated_items.append(result)
        
        # 6. Save to log
        with open(self.output_json, "w", encoding="utf-8") as f:
            json.dump(translated_items, f, indent=2, ensure_ascii=False)
            
        print(f"Processed {len(translated_items)} items. Output saved to {self.output_json}")
        self.assertGreater(len(translated_items), 0)

if __name__ == "__main__":
    unittest.main()
