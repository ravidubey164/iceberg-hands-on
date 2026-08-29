"""
Seed dim_products from the real Coinbase Exchange public product catalog.

No auth required. Endpoint verified reachable 2026-08-02:
https://api.exchange.coinbase.com/products

Usage:
    python setup/fetch_products.py [--status online] [--out dim_products.ndjson]

Writes one JSON line per product (trading pair) to the output file. This is
the slow-changing dimension table in the crossover experiment: real currency
metadata, seeded once and rarely refreshed.
"""
import argparse
import json
import os

import requests

API_URL = "https://api.exchange.coinbase.com/products"
HEADERS = {"User-Agent": "lakehouse-blog-005-research"}
HERE = os.path.dirname(os.path.abspath(__file__))


def fetch_products() -> list[dict]:
    resp = requests.get(API_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", default=None, help="filter by status, e.g. 'online'")
    parser.add_argument("--out", default=os.path.join(HERE, "dim_products.ndjson"))
    args = parser.parse_args()

    products = fetch_products()
    if args.status:
        products = [p for p in products if p.get("status") == args.status]

    with open(args.out, "w") as f:
        for p in products:
            f.write(json.dumps(p) + "\n")

    print(f"[ok] wrote {len(products)} products to {args.out}")


if __name__ == "__main__":
    main()
