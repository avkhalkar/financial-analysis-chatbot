import os
import requests
import time
from datetime import datetime, timedelta



BASE_OUTPUT_DIR = "../../../data"   


CHUNK_SIZE_DAYS = 90
TOTAL_HISTORY_DAYS = 365  

BSE_BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
BSE_PDF_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/corporates/ann.html",
    "Origin": "https://www.bseindia.com",
}



def ensure_dirs(ticker: str) -> str:
  
    path = os.path.join(
        BASE_OUTPUT_DIR,
        ticker,
        "unstructured",
        "raw",
    )
    os.makedirs(path, exist_ok=True)
    return path


def get_date_chunks(days_back: int):
  
    chunks = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(days=CHUNK_SIZE_DAYS)
        if current_end > end_date:
            current_end = end_date

        chunks.append((
            current_start.strftime("%Y%m%d"),
            current_end.strftime("%Y%m%d"),
        ))

        current_start = current_end + timedelta(days=1)

    return chunks



def fetch_bse_metadata_chunk(scrip_code: str, date_from: str, date_to: str):
    params = {
        "strCat": "-1",
        "strPrevDate": date_from,
        "strScrip": scrip_code,
        "strToDate": date_to,
    }

    try:
        r = requests.get(
            BSE_BASE_URL,
            headers=HEADERS,
            params=params,
            timeout=10,
        )
        r.raise_for_status()

        data = r.json()
        return data.get("Table") or data.get("Table1") or []

    except Exception as e:
        print(f"Error fetching {date_from} → {date_to}: {e}")
        return []


def process_company(ticker: str, scrip_code: str):
    print(f"\n--- Processing {ticker} (BSE: {scrip_code}) ---")
    save_dir = ensure_dirs(ticker)

    date_chunks = get_date_chunks(TOTAL_HISTORY_DAYS)
    all_pdfs = []

    for start, end in date_chunks:
        print(f"   -> Querying BSE from {start} to {end}")
        rows = fetch_bse_metadata_chunk(scrip_code, start, end)

        for row in rows:
            fname = row.get("ATTACHMENTNAME")
            subject = row.get("NEWSSUB") or "Document"
            date = row.get("NEWS_DT") or "UnknownDate"

            if fname and fname.lower().endswith(".pdf"):
                all_pdfs.append({
                    "url": BSE_PDF_URL + fname,
                    "subject": subject,
                    "date": date,
                })

        time.sleep(0.5)

    print(f"Found {len(all_pdfs)} PDF filings")

    downloaded = 0
    for doc in all_pdfs:
        safe_subject = "".join(
            c if c.isalnum() else "_"
            for c in doc["subject"][:60]
        )
        safe_date = doc["date"].split("T")[0]
        filename = f"{safe_date}_{safe_subject}.pdf"
        path = os.path.join(save_dir, filename)

        if os.path.exists(path):
            continue

        try:
            r = requests.get(doc["url"], headers=HEADERS, timeout=20)
            r.raise_for_status()
            with open(path, "wb") as f:
                f.write(r.content)
            downloaded += 1
        except Exception:
            pass

    print(f"Downloaded {downloaded} new PDFs")



if __name__ == "__main__":
    targets = [
        {"ticker": "TCS", "scrip": "532540"},
      
    ]

    for t in targets:
        process_company(t["ticker"], t["scrip"])
