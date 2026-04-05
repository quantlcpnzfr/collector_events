from __future__ import annotations

import argparse
import logging


def main() -> None:
    parser = argparse.ArgumentParser(description="collector_events utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    share = subparsers.add_parser("share-server", help="Run lightweight share-link server")
    share.add_argument("--host", default="127.0.0.1")
    share.add_argument("--port", type=int, default=8080)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    if args.command == "share-server":
        from collector_events.share.server import run_share_server

        run_share_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
