from __future__ import annotations

import time
import unittest

from collector_events.share.tokens import ShareTokenError, create_signed_token, verify_signed_token


class ShareTokenTests(unittest.TestCase):
    def test_round_trip_token(self) -> None:
        token = create_signed_token({"type": "x", "snapshot": {"as_of": "now"}}, secret="secret", ttl_seconds=60)
        payload = verify_signed_token(token, secret="secret")
        self.assertEqual(payload["type"], "x")
        self.assertIn("exp", payload)

    def test_tampered_token_is_rejected(self) -> None:
        token = create_signed_token({"type": "x"}, secret="secret", ttl_seconds=60)
        tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
        with self.assertRaises(ShareTokenError):
            verify_signed_token(tampered, secret="secret")

    def test_expired_token_is_rejected(self) -> None:
        now = int(time.time())
        token = create_signed_token({"type": "x"}, secret="secret", ttl_seconds=1)
        with self.assertRaises(ShareTokenError):
            verify_signed_token(token, secret="secret", now=now + 3600)


if __name__ == "__main__":
    unittest.main()

