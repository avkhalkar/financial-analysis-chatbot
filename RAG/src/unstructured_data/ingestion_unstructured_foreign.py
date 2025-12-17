""" 

** Please Read Once before Modifying or Using ** ~DhanushHN10
THIS SCRIPT IS AN INGESTION WORKER FOR *US / SEC-LISTED COMPANIES ONLY*.

It MUST be called by an upstream system that has already:
1. Resolved the user prompt into a concrete company entity
2. Determined the jurisdiction is the United States
3. Supplied a VALID SEC CIK for that company

This script DOES NOT:
- Guess company identity
- Guess country
- Decide which ingestion pipeline to use
- Handle Indian companies (BSE/NSE)
- Handle user prompts directly

If a non-US company or an invalid CIK is passed here,
the script is expected to FAIL FAST to prevent RAG corruption.

Output contract (MANDATORY, DO NOT CHANGE):
data/
  └── {COMPANY}/
      ├── structured/
      │   └── data.json        
      └── unstructured/
          └── data.json        

The output file contains BOTH content and metadata.
This script always fetches the latest available 10-K at runtime.
"""

import os
import json
import requests
import re
from datetime import datetime, timezone



BASE_DIR = "../../../data"

HEADERS = {
    "User-Agent": "FinancialRAGBot/1.0 aiclub@iitdh.ac.in",
    "Accept-Encoding": "gzip, deflate",
}

MAX_TOTAL_CHARS = 40_000
MAX_SECTION_CHARS = 10_000

SECTION_PATTERNS = [
    r"Item\s+7\.\s+Management",     # MD&A 
    r"Item\s+1A\.\s+Risk",          # Risk factors
    r"Item\s+1\.\s+Business",       # Business & segments
    r"Item\s+7A\.",                 # Market risk 
 
]


def get_latest_10k_metadata(cik: str) -> dict | None:
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()

    filings = r.json().get("filings", {}).get("recent", {})

    for form, acc, primary, filing_date in zip(
        filings.get("form", []),
        filings.get("accessionNumber", []),
        filings.get("primaryDocument", []),
        filings.get("filingDate", []),
    ):
        if form == "10-K":
            return {
                "accession": acc.replace("-", ""),
                "primary_doc": primary,
                "filing_date": filing_date,
            }

    return None


def fetch_filing_html(cik: str, accession: str, primary_doc: str) -> str:
    url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession}/{primary_doc}"
    )
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.text


def extract_high_signal_sections(text: str) -> str:
    extracted = []
    total_chars = 0

    for pattern in SECTION_PATTERNS:
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue

        chunk = text[
            match.start() : match.start() + MAX_SECTION_CHARS
        ]

        extracted.append(chunk)
        total_chars += len(chunk)

        if total_chars >= MAX_TOTAL_CHARS:
            break

    if not extracted:
        return text[:MAX_TOTAL_CHARS]

    return "\n\n".join(extracted)[:MAX_TOTAL_CHARS]



def ingest_sec_unstructured(*, ticker: str, cik: str) -> dict:
    if not cik.isdigit():
        raise ValueError("Invalid CIK. SEC ingestion aborted.")

    meta = get_latest_10k_metadata(cik)
    if not meta:
        raise RuntimeError("No 10-K filing found for provided CIK.")

    html = fetch_filing_html(
        cik=cik,
        accession=meta["accession"],
        primary_doc=meta["primary_doc"],
    )

    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    text = extract_high_signal_sections(text)

    record = {
        "company": ticker,
        "jurisdiction": "US",
        "source": "SEC EDGAR",
        "filing_type": "10-K",
        "filing_date": meta["filing_date"],
        "accession": meta["accession"],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "data_version": "v1.0",
        "text": text,
    }

    out_dir = os.path.join(BASE_DIR, ticker, "unstructured")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)

    return record


if __name__ == "__main__":
    result = ingest_sec_unstructured(
        ticker="MSFT",
        cik="0000789019",
    )

    print("Company:", result["company"])
    print("Filing date:", result["filing_date"])
    print("Characters:", len(result["text"]))
    print("Saved to:", f"{BASE_DIR}/MSFT/unstructured/data.json")
