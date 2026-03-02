# HBTU University RAG System

A precision-engineered Retrieval-Augmented Generation (RAG) system built for institutional knowledge extraction from noisy HTML and OCR-based PDF data.

## Features

- Boilerplate detection & removal
- OCR noise filtering
- Date-based precision filtering (last 3 years)
- Duplicate elimination (90% similarity threshold)
- Structured JSON knowledge chunk generation

## Architecture

Raw Data → Cleaning → Deduplication → Date Filtering → Structured JSON → RAG Indexing

## Tech Stack

- Python
- BeautifulSoup
- FAISS (planned)
- SentenceTransformers (planned)
- FastAPI (planned)

## Status

Reconstruction Engine: ✅
Hybrid Retriever: ⏳
API Layer: ⏳