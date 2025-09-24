import logging
import os
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup

# Custom headers to mimic a real browser request
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

VERIFY_SSL = os.getenv("CRAWLER_VERIFY_SSL", "true").lower() not in {
    "0",
    "false",
    "no",
}

_SESSION = requests.Session()
_SESSION.headers.update(HEADERS)
_SESSION.verify = VERIFY_SSL

logger = logging.getLogger(__name__)


def crawl_and_download(
    start_url: str,
    download_folder: Path,
    retries: int = 3,
    delay: int = 5,
    *,
    allowed_hosts: Optional[Iterable[str]] = None,
) -> List[Dict[str, str]]:
    """Crawl a website starting from ``start_url`` and download every PDF that is found.

    Parameters
    ----------
    start_url:
        The first page that will be crawled.
    download_folder:
        The directory where downloaded PDFs should be stored.
    retries:
        Number of attempts to make when a request fails due to timeout or transient
        issues.
    delay:
        Number of seconds to wait between retries.

    allowed_hosts:
        Optional iterable of hostnames that are allowed during the crawl. When
        provided, only links that resolve to these hosts will be followed. This
        is useful for intranet environments to avoid accidentally crawling the
        public internet when offline mirrors link externally.

    Returns
    -------
    list of dict
        Metadata for every PDF that was downloaded. Each item contains ``url``,
        ``path`` (local path on disk), ``filename`` and ``downloaded_at``. The page
        where the file was discovered is stored under ``source_page``.
    """

    download_folder = Path(download_folder)
    download_folder.mkdir(parents=True, exist_ok=True)

    visited = set()
    queue: deque[str] = deque([start_url])
    downloaded: List[Dict[str, str]] = []
    allowed: Optional[Set[str]] = None
    if allowed_hosts:
        allowed = set()
        for host in allowed_hosts:
            lowered = host.lower()
            allowed.add(lowered)
            if ":" in lowered:
                allowed.add(lowered.split(":", 1)[0])

    while queue:
        current_url = queue.popleft()
        if current_url in visited:
            continue
        visited.add(current_url)

        response = _request_with_retries(current_url, retries=retries, delay=delay)
        if response is None:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.select("a[href]"):
            href = link.get("href")
            if not href:
                continue

            href, _fragment = urldefrag(href)
            full_url = urljoin(current_url, href)
            parsed = urlparse(full_url)

            if parsed.scheme not in {"http", "https"}:
                continue

            netloc = parsed.netloc.lower()
            hostname = parsed.hostname.lower() if parsed.hostname else ""
            if allowed and netloc not in allowed and hostname not in allowed:
                continue

            if parsed.path.lower().endswith(".pdf"):
                pdf_info = download_pdf(full_url, download_folder)
                if pdf_info:
                    pdf_info["source_page"] = current_url
                    downloaded.append(pdf_info)
            elif full_url not in visited:
                queue.append(full_url)

    return downloaded


def _request_with_retries(
    url: str,
    retries: int = 3,
    delay: int = 5,
) -> Optional[requests.Response]:
    """Fetch ``url`` while retrying transient failures."""

    attempt = 0
    while attempt < retries:
        try:
            response = _SESSION.get(url, timeout=15)
            if response.status_code == 200:
                logger.info("Successfully accessed %s", url)
                return response

            if response.status_code in (403, 404):
                logger.warning("%s returned status %s", url, response.status_code)
                return None

            logger.warning(
                "Failed to access %s, status code %s", url, response.status_code
            )
            return None
        except requests.exceptions.Timeout:
            attempt += 1
            logger.warning(
                "Timeout while requesting %s (attempt %s/%s)", url, attempt, retries
            )
            time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.error("Request error for %s: %s", url, exc)
            return None

    logger.error("Giving up on %s after %s attempts", url, retries)
    return None


def download_pdf(url: str, folder: Path) -> Optional[Dict[str, str]]:
    """Download a PDF file and return metadata about it."""

    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    parsed = urlparse(url)
    pdf_name = os.path.basename(parsed.path) or "downloaded.pdf"
    target_path = folder / pdf_name

    if target_path.exists():
        logger.info("%s already exists, skipping download", target_path)
        return {
            "url": url,
            "path": str(target_path),
            "filename": pdf_name,
            "downloaded_at": datetime.utcfromtimestamp(target_path.stat().st_mtime)
            .isoformat()
            + "Z",
        }

    try:
        response = _SESSION.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.warning("Timeout while downloading %s", url)
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to download %s: %s", url, exc)
        return None

    with open(target_path, "wb") as file_pointer:
        file_pointer.write(response.content)

    logger.info("Downloaded %s", pdf_name)
    return {
        "url": url,
        "path": str(target_path),
        "filename": pdf_name,
        "downloaded_at": datetime.utcnow().isoformat() + "Z",
    }
