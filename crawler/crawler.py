import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import os
import time

# Custom headers to mimic a real browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def crawl_and_download(start_url, download_folder, retries=3, delay=5):
    visited = set()
    to_visit = [start_url]
    
    while to_visit:
        url = to_visit.pop(0)
        if url not in visited:
            visited.add(url)

            attempt = 0
            while attempt < retries:
                try:
                    response = requests.get(url, timeout=10, headers=headers)  # Added headers here

                    if response.status_code == 404:
                        print(f"404 Not Found for URL: {url}. Skipping...")
                        break

                    if response.status_code == 403:
                        print(f"403 Forbidden for URL: {url}. Skipping...")
                        break

                    soup = BeautifulSoup(response.text, "html.parser")

                    if response.status_code == 200:
                        print(f"Successfully accessed: {url}")
                        break
                    else:
                        print(f"Failed to access {url}, Status Code: {response.status_code}")
                        break

                except requests.exceptions.Timeout:
                    print(f"Timeout error for URL: {url}. Retrying {attempt + 1}/{retries}...")
                    attempt += 1
                    time.sleep(delay)  # Wait for a few seconds before retrying

                except requests.exceptions.RequestException as e:
                    print(f"Request error for URL: {url}. Error: {e}. Skipping...")
                    break

            # Find all links on the page if the page was accessed successfully
            if response.status_code == 200:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    full_url = urljoin(url, href)

                    # If it's a PDF, download it
                    if full_url.endswith('.pdf'):
                        print(f"PDF found: {full_url}")  # Print the PDF URL for debugging
                        download_pdf(full_url, download_folder)

                    # Add new links to the list to visit (if they are valid and not visited yet)
                    if full_url not in visited and "http" in full_url:
                        to_visit.append(full_url)

def download_pdf(url, folder):
    try:
        response = requests.get(url, timeout=10, headers=headers)  # Added headers here
        pdf_name = url.split("/")[-1]
        path = os.path.join(folder, pdf_name)

        # Save the PDF file
        with open(path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {pdf_name}")  # Print confirmation for downloaded PDFs
        
    except requests.exceptions.Timeout:
        print(f"Timeout error while downloading {url}. Skipping...")
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading {url}. Error: {e}. Skipping...")
