import logging
import os
from pathlib import Path
from typing import Dict, Iterable, Mapping
from urllib.parse import urlparse, urlunparse

from flask import Flask, render_template, request

from crawler.crawler import crawl_and_download
from elasticsearch_index.es_index import (
    build_document_metadata,
    create_index,
    index_multiple,
    search_pdfs,
)
from storage.manifest import (
    Manifest,
    load_manifest,
    pending_documents,
    update_manifest,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "change-me")
app.config["DOWNLOAD_DIR"] = Path(os.getenv("DOWNLOAD_DIR", "./downloaded_pdfs")).resolve()
app.config["MANIFEST_PATH"] = app.config["DOWNLOAD_DIR"] / "manifest.json"


def _filesize(value: object) -> str:
    try:
        size = float(value)
    except (TypeError, ValueError):
        return "-"

    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    if index == 0:
        return f"{int(size)} {units[index]}"
    return f"{size:.2f} {units[index]}"


app.jinja_env.filters["filesize"] = _filesize


def _load_manifest() -> Manifest:
    return load_manifest(app.config["DOWNLOAD_DIR"])


def _render_home(**context):
    context.setdefault("manifest", _load_manifest())
    context.setdefault("manifest_path", str(app.config["MANIFEST_PATH"]))
    return render_template("index.html", **context)


def _update_manifest(records: Iterable[Mapping[str, object]]) -> Manifest:
    return update_manifest(app.config["DOWNLOAD_DIR"], records)


try:
    create_index()
except RuntimeError as exc:
    logger.warning("Elasticsearch unavailable during startup: %s", exc)


@app.route("/")
def index():
    return _render_home()


def _normalize_start_url(raw_url: str) -> str:
    """Return a fully qualified URL for crawling."""

    parsed = urlparse(raw_url)
    if not parsed.scheme:
        # Default to HTTP for intranet/offline sites unless a scheme is provided.
        parsed = parsed._replace(scheme="http")
    if not parsed.netloc:
        # In case the user passed only a hostname without scheme.
        parsed = urlparse(f"{parsed.scheme}://{parsed.path}")
    if not parsed.netloc:
        raise ValueError("A valid hostname is required to start crawling.")
    return urlunparse(parsed)


@app.post("/start_scraping")
def start_scraping():
    website_url = request.form.get("url", "").strip()
    if not website_url:
        return _render_home(error="A website URL is required."), 400

    try:
        start_url = _normalize_start_url(website_url)
    except ValueError as exc:
        return _render_home(error=str(exc)), 400

    download_folder: Path = app.config["DOWNLOAD_DIR"]
    download_folder.mkdir(parents=True, exist_ok=True)

    allowed_hosts = {urlparse(start_url).netloc}

    downloaded_documents = crawl_and_download(
        start_url,
        download_folder,
        allowed_hosts=allowed_hosts,
    )
    pending_to_index = pending_documents(_load_manifest())

    try:
        create_index()
        pending_stats = (
            index_multiple(pending_to_index) if pending_to_index else _empty_index_stats()
        )
        new_stats = (
            index_multiple(downloaded_documents)
            if downloaded_documents
            else _empty_index_stats()
        )
        manifest = _update_manifest(
            [*pending_stats["documents"], *new_stats["documents"]]
        )
        totals = _combine_index_stats(pending_stats, new_stats)
    except RuntimeError as exc:
        logger.error("Failed to index PDFs: %s", exc)
        manifest = _update_manifest(
            _pending_records(downloaded_documents)
        )
        pending_stats = _empty_index_stats()
        new_stats = _empty_index_stats()
        totals = _combine_index_stats(pending_stats, new_stats)
        error_message = (
            "Failed to index PDFs because Elasticsearch is unavailable. "
            "Downloads are recorded locally and can be re-indexed later."
        )
        return _render_home(error=error_message, manifest=manifest), 502

    message = {
        "website_url": start_url,
        "downloaded": len(downloaded_documents),
        "indexed": totals["indexed"],
        "duplicates": totals["duplicates"],
        "skipped": totals["skipped"],
        "indexed_new": new_stats["indexed"],
        "duplicates_new": new_stats["duplicates"],
        "skipped_new": new_stats["skipped"],
        "reindexed_pending": pending_stats["indexed"],
        "library_total": manifest.stats["total"],
    }

    return _render_home(message=message, manifest=manifest)


@app.route("/search", methods=["GET", "POST"])
def search():
    query = request.values.get("query", "").strip()
    results = []
    error = None
    if query:
        try:
            results = search_pdfs(query)
        except RuntimeError as exc:
            logger.error("Search failed: %s", exc)
            error = (
                "Elasticsearch is unavailable. Please try again once the "
                "cluster is reachable."
            )

    return render_template(
        "search_results.html",
        query=query,
        results=results,
        error=error,
    )


def _pending_records(documents: Iterable[Mapping[str, object]]):
    for doc in documents:
        try:
            metadata = build_document_metadata(
                doc["path"],
                doc["url"],
                source_page=doc.get("source_page"),
                downloaded_at=doc.get("downloaded_at"),
            )
        except FileNotFoundError:
            continue
        metadata.update({"indexed": False, "status": "not_indexed"})
        yield metadata


def _empty_index_stats() -> Dict[str, object]:
    return {
        "indexed": 0,
        "duplicates": 0,
        "skipped": 0,
        "documents": [],
    }


def _combine_index_stats(*stats_groups: Mapping[str, object]) -> Dict[str, int]:
    totals: Dict[str, int] = {"indexed": 0, "duplicates": 0, "skipped": 0}
    for stats in stats_groups:
        totals["indexed"] += int(stats.get("indexed", 0))
        totals["duplicates"] += int(stats.get("duplicates", 0))
        totals["skipped"] += int(stats.get("skipped", 0))
    return totals


if __name__ == "__main__":
    app.run(debug=True)
