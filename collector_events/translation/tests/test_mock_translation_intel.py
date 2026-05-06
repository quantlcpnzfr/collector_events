import asyncio
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from dataclasses import asdict

from forex_shared.domain.intel import IntelItem
from collector_events.translation.engine import TranslationEngine
from collector_events.translation.session import TranslationSession

class TestTranslationIntelEnrichment(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = Path(r"C:\Projects\forex_system\services\collector_events\collector_events\translation\tests")
        self.source_json = Path(r"C:\Projects\forex_system\services\collector_events\collector_events\extractors\osint_telegram\logs\osint_feed_intel.json")
        self.output_json = self.test_dir / "osint_feed_translated_intel.json"
        
        # Ensure test dir exists
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        # Mock Config
        self.config = {
            "session_id": "test-intel-session",
            "detector_model_path": ".models/translation/glotlid/model.bin",
            "translation_provider": "nllb", 
            "min_detection_confidence": 0.1,
        }
        
        # Mock MQ
        self.mock_mq = AsyncMock()
        
    @patch("forex_shared.providers.mq.mq_factory.MQFactory.create_async_from_env")
    async def test_intel_item_translation_flow(self, mock_mq_factory):
        mock_mq_factory.return_value = self.mock_mq
        
        # 1. Read source JSON
        if not self.source_json.exists():
            self.skipTest(f"Source file {self.source_json} not found")
            
        with open(self.source_json, "r", encoding="utf-8") as f:
            raw_items = json.load(f)
            
        # 2. Transform to IntelItem (simulating real object flow)
        # Note: We use dict for internal processing but ensure it maps to IntelItem fields
        intel_items = []
        for item in raw_items:
            try:
                # Filter item to match IntelItem fields if necessary, or just use dict
                # The session expects a dict for _process_payload as it simulates an MQ payload
                intel_items.append(item)
            except Exception as e:
                print(f"Error parsing item: {e}")

        # 3. Setup Session with Mocked Engine
        engine = TranslationEngine(self.config)
        
        # Mock detection logic
        def mock_detect(text):
            # Persian detection mock (check for Arabic-script Persian text)
            if any('\u0600' <= c <= '\u06FF' for c in text):
                return MagicMock(language="pes_Arab", language_name="Persian", confidence=0.9, status="detected", error="")

            # Hebrew detection mock (check for Hebrew characters)
            if any('\u0590' <= c <= '\u05FF' for c in text):
                return MagicMock(language="heb_Hebr", language_name="Hebrew", confidence=0.9, status="detected", error="")
                
            # Ukrainian/Russian detection mock (check for Cyrillic characters)
            if any('\u0400' <= c <= '\u04FF' for c in text):
                return MagicMock(language="ukr_Cyrl", language_name="Ukrainian", confidence=0.9, status="detected", error="")

            return MagicMock(language="eng_Latn", language_name="English", confidence=1.0, status="detected", error="")
        
        engine.detect_language = MagicMock(side_effect=mock_detect)
        
        # Mock translation logic
        def mock_translate(text, lang):
            if "שר החוץ הגרמני" in text:
                eng = "Germany's Foreign Minister (during Foreign Minister Gideon Sa'ar's visit to Germany): \n\nGermany is committed to Israel's security - a steadfast commitment that is not subject to change.\n\nLet it always be clear: we will not allow any threat to the very existence of the State of Israel.\n\nThis is a fundamental principle of Germany's policy."
                return eng, "translated", ""

            compact = " ".join(str(text).split())
            return f"[MOCK TRANSLATED from {lang}] {compact}", "translated", ""
        engine.translate = MagicMock(side_effect=mock_translate)
        
        session = TranslationSession(self.config, engine=engine)
        session._mq = self.mock_mq
        
        results = []
        
        # 4. Process items
        for item in intel_items:
            # Simulate MQ receiving a serialized IntelItem
            processed = await asyncio.to_thread(session._process_payload, item)
            if processed:
                # 0. Check enrichment contract
                self.assertNotIn("title_en", processed)
                self.assertNotIn("body_en", processed)
                self.assertNotIn("text_en", processed)
                self.assertNotIn("event_type", processed)
                self.assertNotIn("translation", processed)
                self.assertNotIn("source_language", processed)
                self.assertNotIn("source_language_name", processed)
                
                self.assertIn("translation_source", processed["extra"])
                self.assertIn("original_body", processed["extra"])
                
                source_str = processed["extra"]["translation_source"]
                if "⚡Translation from" in source_str:
                    # Verify original_body exists for non-english
                    self.assertEqual(processed["extra"]["original_body"], item["body"])
                else:
                    # Verify original_body is empty string for english
                    self.assertEqual(processed["extra"]["original_body"], "")
                
                # Check title length and format (no newlines)
                self.assertLessEqual(len(processed["title"]), 205)
                self.assertNotIn("\n", processed["title"])
                
                results.append(processed)
        
        # 6. Log the results into a JSON array of IntelItem formatted objects
        with open(self.output_json, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
            
        print(f"Processed {len(results)} items. Results logged to {self.output_json}")
        self.assertGreater(len(results), 0)

if __name__ == "__main__":
    unittest.main()
