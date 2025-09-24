import hashlib
import logging
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from elasticsearch import Elasticsearch, exceptions
from pdfminer.high_level import extract_text

logger = logging.getLogger(__name__)

ES_INDEX = "pdfs"


@lru_cache(maxsize=1)
def get_es_client() -> Elasticsearch:
    """Return a cached Elasticsearch client using environment variables."""

    host = os.getenv("ES_HOST", "localhost")
    port = int(os.getenv("ES_PORT", "9200"))
    scheme = os.getenv("ES_SCHEME", "http")

    client = Elasticsearch(
        hosts=[{"host": host, "port": port, "scheme": scheme}],
        request_timeout=30,
    )

    try:
        if not client.ping():
            raise RuntimeError("Unable to connect to Elasticsearch")
    except exceptions.ConnectionError as exc:  # pragma: no cover - connection issues
        raise RuntimeError("Unable to connect to Elasticsearch") from exc

    return client


def create_index(client: Optional[Elasticsearch] = None) -> None:
    """Create the Elasticsearch index with the appropriate mapping."""

    es = client or get_es_client()
    if es.indices.exists(index=ES_INDEX):
        return

    index_mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "size": {"type": "long"},
                "url": {"type": "keyword"},
                "source_page": {"type": "keyword"},
                "downloaded_at": {"type": "date"},
                "sha256": {"type": "keyword"},
                "content": {"type": "text", "analyzer": "english"},
            }
        },
    }

    es.indices.create(index=ES_INDEX, body=index_mapping)
    logger.info("Created Elasticsearch index '%s'", ES_INDEX)


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract textual content from a PDF file."""

    try:
        return extract_text(pdf_path)
    except Exception as exc:  # pragma: no cover - pdfminer specific errors
        logger.warning("Failed to extract text from %s: %s", pdf_path, exc)
        return ""


def file_sha256(pdf_path: Path) -> str:
    hasher = hashlib.sha256()
    with open(pdf_path, "rb") as file_pointer:
        for chunk in iter(lambda: file_pointer.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_document_metadata(
    pdf_path: str,
    pdf_url: str,
    *,
    source_page: Optional[str] = None,
    downloaded_at: Optional[str] = None,
) -> Dict[str, object]:
    """Return metadata describing a PDF document on disk."""

    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing PDF at {pdf_path}")

    doc_hash = file_sha256(path)
    metadata: Dict[str, object] = {
        "id": doc_hash,
        "sha256": doc_hash,
        "name": path.name,
        "path": str(path.resolve()),
        "size": path.stat().st_size,
        "url": pdf_url,
        "source_page": source_page,
        "downloaded_at": downloaded_at or datetime.utcnow().isoformat() + "Z",
    }
    return metadata


def index_pdf(
    pdf_path: str,
    pdf_url: str,
    *,
    source_page: Optional[str] = None,
    downloaded_at: Optional[str] = None,
    client: Optional[Elasticsearch] = None,
) -> Optional[Dict[str, object]]:
    """Index a PDF file and return manifest metadata."""

    path = Path(pdf_path)
    if not path.exists():
        logger.warning("Cannot index missing file %s", pdf_path)
        return None

    es = client or get_es_client()

    try:
        metadata = build_document_metadata(
            pdf_path,
            pdf_url,
            source_page=source_page,
            downloaded_at=downloaded_at,
        )
    except FileNotFoundError:
        logger.warning("Cannot index missing file %s", pdf_path)
        return None

    pdf_content = extract_pdf_text(path)
    if not pdf_content.strip():
        logger.info("No text extracted from %s; skipping indexing", path.name)
        metadata.update({"indexed": False, "status": "no_text"})
        return metadata

    doc_hash = metadata["sha256"]
    if es.exists(index=ES_INDEX, id=doc_hash):
        logger.info("Document %s already indexed", path.name)
        metadata.update({"indexed": False, "status": "duplicate"})
        return metadata

    document = dict(metadata)
    document["content"] = pdf_content

    es.index(index=ES_INDEX, id=doc_hash, document=document, refresh="wait_for")
    logger.info("Indexed %s", path.name)
    metadata.update({"indexed": True, "status": "indexed"})
    return metadata


def index_multiple(
    documents: Iterable[dict],
    client: Optional[Elasticsearch] = None,
) -> Dict[str, object]:
    """Index multiple PDFs and return statistics with manifest records."""

    es = client or get_es_client()
    indexed = 0
    duplicates = 0
    skipped = 0
    records: List[Dict[str, object]] = []

    for doc in documents:
        result = index_pdf(
            doc["path"],
            doc["url"],
            source_page=doc.get("source_page"),
            downloaded_at=doc.get("downloaded_at"),
            client=es,
        )
        if not result:
            continue

        records.append(result)
        status = result.get("status")
        if status == "indexed":
            indexed += 1
        elif status == "duplicate":
            duplicates += 1
        else:
            skipped += 1

    return {
        "indexed": indexed,
        "duplicates": duplicates,
        "skipped": skipped,
        "documents": records,
    }


def search_pdfs(
    query: str,
    *,
    size: int = 20,
    client: Optional[Elasticsearch] = None,
):
    """Search the indexed PDFs and return Elasticsearch hits."""

    if not query:
        return []

    es = client or get_es_client()
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["name^2", "content", "url", "source_page"],
                "type": "best_fields",
            }
        },
        "highlight": {
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
            "fields": {"content": {"fragment_size": 200, "number_of_fragments": 1}},
        },
    }

    response = es.search(index=ES_INDEX, body=body)
    return response["hits"]["hits"]
