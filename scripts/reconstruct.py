import os
import zipfile
import re
import json
import hashlib
from collections import Counter, defaultdict
from bs4 import BeautifulSoup
from datetime import datetime
from difflib import SequenceMatcher

# -----------------------
# CONFIG
# -----------------------

CURRENT_YEAR = datetime.now().year
NOTICE_YEAR_THRESHOLD = CURRENT_YEAR - 3
MIN_CONTENT_LENGTH = 200
BOILERPLATE_FREQ_THRESHOLD = 0.4  # 40%
SIMILARITY_THRESHOLD = 0.9

INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------
# UTILITY FUNCTIONS
# -----------------------

def unzip_all():
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(INPUT_DIR, file), 'r') as zip_ref:
                zip_ref.extractall(os.path.join(INPUT_DIR, file.replace(".zip", "")))


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
                return year
            except:
                continue
    return None


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


# -----------------------
# HTML PROCESSING
# -----------------------

def clean_html_file(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")

    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    text = normalize_text(text)
    return text


# -----------------------
# TXT PROCESSING
# -----------------------

def clean_txt_file(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    text = normalize_text(text)
    return text


# -----------------------
# MAIN RECONSTRUCTION
# -----------------------

def reconstruct():
    unzip_all()

    all_docs = []
    boilerplate_counter = Counter()

    # STEP 1: Load all documents
    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            full_path = os.path.join(root, file)

            if file.endswith(".html"):
                text = clean_html_file(full_path)
            elif file.endswith(".txt"):
                text = clean_txt_file(full_path)
            else:
                continue

            if len(text) < MIN_CONTENT_LENGTH:
                continue

            all_docs.append({
                "path": full_path,
                "content": text
            })

            # collect line frequencies for boilerplate detection
            lines = text.split(". ")
            for line in lines:
                boilerplate_counter[line.strip()] += 1

    # STEP 2: Identify boilerplate lines
    boilerplate_lines = set()
    total_docs = len(all_docs)

    for line, count in boilerplate_counter.items():
        if count / total_docs > BOILERPLATE_FREQ_THRESHOLD:
            boilerplate_lines.add(line)

    # STEP 3: Remove boilerplate
    cleaned_docs = []
    for doc in all_docs:
        lines = doc["content"].split(". ")
        filtered = [l for l in lines if l not in boilerplate_lines]
        final_text = ". ".join(filtered)

        if len(final_text) < MIN_CONTENT_LENGTH:
            continue

        cleaned_docs.append({
            "path": doc["path"],
            "content": final_text
        })

    # STEP 4: Date filtering
    filtered_docs = []
    for doc in cleaned_docs:
        year = extract_date(doc["content"])
        if year:
            if year < NOTICE_YEAR_THRESHOLD:
                continue

        filtered_docs.append(doc)

    # STEP 5: Deduplicate
    final_docs = []
    for doc in filtered_docs:
        duplicate = False
        for existing in final_docs:
            if similarity(doc["content"], existing["content"]) > SIMILARITY_THRESHOLD:
                duplicate = True
                break
        if not duplicate:
            final_docs.append(doc)

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

    print("Reconstruction complete.")
    print("Total final documents:", len(final_docs))


if __name__ == "__main__":
    reconstruct()