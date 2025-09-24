import logging
import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from flask import Flask, render_template, request

from crawler.crawler import crawl_and_download
from elasticsearch_index.es_index import create_index, index_multiple, search_pdfs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "change-me")
app.config["DOWNLOAD_DIR"] = Path(os.getenv("DOWNLOAD_DIR", "./downloaded_pdfs")).resolve()

# Ensure the Elasticsearch index exists when the application starts
create_index()


@app.route("/")
def index():
    return render_template("index.html")


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
        return (
            render_template("index.html", error="A website URL is required."),
            400,
        )

    try:
        start_url = _normalize_start_url(website_url)
    except ValueError as exc:
        return render_template("index.html", error=str(exc)), 400

    download_folder: Path = app.config["DOWNLOAD_DIR"]
    download_folder.mkdir(parents=True, exist_ok=True)

    allowed_hosts = {urlparse(start_url).netloc}

    downloaded_documents = crawl_and_download(
        start_url,
        download_folder,
        allowed_hosts=allowed_hosts,
    )
    indexed_count = index_multiple(downloaded_documents)

    message = {
        "website_url": start_url,
        "downloaded": len(downloaded_documents),
        "indexed": indexed_count,
    }

    return render_template("index.html", message=message)


@app.route("/search", methods=["GET", "POST"])
def search():
    query = request.values.get("query", "").strip()
    results = search_pdfs(query) if query else []

    return render_template("search_results.html", query=query, results=results)


if __name__ == "__main__":
    app.run(debug=True)
