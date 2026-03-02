import os
import zipfile
import re
import json
import hashlib
from collections import Counter
from bs4 import BeautifulSoup
from datetime import datetime
import logging

# -----------------------
# CONFIG
# -----------------------

CURRENT_YEAR = datetime.now().year
NOTICE_YEAR_THRESHOLD = CURRENT_YEAR - 3
MIN_CONTENT_LENGTH = 200
BOILERPLATE_FREQ_THRESHOLD = 0.4  # 40%

INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed"

os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILE = "logs/reconstruction_log.txt"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# UTILITY FUNCTIONS
# -----------------------

def unzip_all():
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".zip"):
            extract_path = os.path.join(INPUT_DIR, file.replace(".zip", ""))
            if not os.path.exists(extract_path):
                with zipfile.ZipFile(os.path.join(INPUT_DIR, file), 'r') as zip_ref:
                    zip_ref.extractall(extract_path)

def normalize_text(text):
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_date(text):
    patterns = [
        r"\b\d{2}[/-]\d{2}[/-]\d{4}\b",
        r"\b\d{4}\b"
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                year = int(match.group()[-4:])
                if 2000 <= year <= CURRENT_YEAR:
                    return year
            except:
                continue
    return None

# -----------------------
# HTML PROCESSING
# -----------------------

def clean_html_file(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")

    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    lines = [normalize_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    return lines

# -----------------------
# TXT PROCESSING
# -----------------------

def clean_txt_file(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    lines = [normalize_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    return lines

# -----------------------
# MAIN RECONSTRUCTION
# -----------------------

def reconstruct():
    unzip_all()
    logging.info("Unzipping completed.")

    all_docs = []
    boilerplate_counter = Counter()

    # STEP 1: Load documents
    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            full_path = os.path.join(root, file)

            if file.endswith(".html"):
                lines = clean_html_file(full_path)
            elif file.endswith(".txt"):
                lines = clean_txt_file(full_path)
            else:
                continue

            full_text = " ".join(lines)

            if len(full_text) < MIN_CONTENT_LENGTH:
                continue

            all_docs.append({
                "path": full_path,
                "lines": lines,
                "content": full_text
            })

            for line in lines:
                boilerplate_counter[line] += 1

    logging.info(f"Total raw documents loaded: {len(all_docs)}")

    if not all_docs:
        logging.warning("No documents found. Exiting.")
        return

    # STEP 2: Identify boilerplate lines
    boilerplate_lines = set()
    total_docs = len(all_docs)

    for line, count in boilerplate_counter.items():
        if count / total_docs > BOILERPLATE_FREQ_THRESHOLD:
            boilerplate_lines.add(line)

    logging.info(f"Boilerplate lines detected: {len(boilerplate_lines)}")

    # STEP 3: Remove boilerplate
    cleaned_docs = []
    for doc in all_docs:
        filtered_lines = [l for l in doc["lines"] if l not in boilerplate_lines]
        final_text = " ".join(filtered_lines)

        if len(final_text) < MIN_CONTENT_LENGTH:
            continue

        cleaned_docs.append({
            "path": doc["path"],
            "content": final_text
        })

    logging.info(f"Documents after boilerplate removal: {len(cleaned_docs)}")

    # STEP 4: Date filtering (only for notices with detected year)
    filtered_docs = []
    for doc in cleaned_docs:
        year = extract_date(doc["content"])
        if year and year < NOTICE_YEAR_THRESHOLD:
            continue
        filtered_docs.append(doc)

    logging.info(f"Documents after date filtering: {len(filtered_docs)}")

    # STEP 5: Fast Hash-Based Deduplication (O(n))
    final_docs = []
    seen_hashes = set()

    for doc in filtered_docs:
        normalized = normalize_text(doc["content"])
        content_hash = hashlib.md5(normalized.encode()).hexdigest()

        if content_hash not in seen_hashes:
            seen_hashes.add(content_hash)
            final_docs.append(doc)

    logging.info(f"Documents after hash deduplication: {len(final_docs)}")

    # STEP 6: Save structured JSON
    output_path = os.path.join(OUTPUT_DIR, "knowledge_chunks.jsonl")

    with open(output_path, "w", encoding="utf-8") as f:
        for doc in final_docs:
            chunk = {
                "id": hashlib.md5(doc["path"].encode()).hexdigest(),
                "title": os.path.basename(doc["path"]),
                "content": doc["content"],
                "priority": 3
            }
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")

    logging.info("knowledge_chunks.jsonl successfully generated.")

    print("Reconstruction complete.")
    print("Total final documents:", len(final_docs))


if __name__ == "__main__":
    reconstruct()