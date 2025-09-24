from elasticsearch import Elasticsearch
from pdfminer.high_level import extract_text
import os

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Create an index for storing PDF metadata and content
def create_index():
    if not es.indices.exists(index="pdfs"):
        index_mapping = {
            "mappings": {
                "properties": {
                    "name": {"type": "text"},
                    "size": {"type": "long"},
                    "url": {"type": "text"},
                    "content": {"type": "text"},
                }
            }
        }
        es.indices.create(index='pdfs', body=index_mapping, ignore=400)
        print("Index created.")

# Extract text from PDFs
def extract_pdf_text(pdf_path):
    try:
        text = extract_text(pdf_path)
        return text
    except Exception as e:
        print(f"Failed to extract text from {pdf_path}: {e}")
        return ""

# Index PDF into Elasticsearch
def index_pdf(pdf_path, pdf_url):
    pdf_name = os.path.basename(pdf_path)
    pdf_size = os.path.getsize(pdf_path)
    pdf_content = extract_pdf_text(pdf_path)

    # Ensure the content is not empty
    if not pdf_content:
        print(f"Warning: No text extracted from {pdf_name}. Skipping indexing.")
        return

    doc = {
        'name': pdf_name,
        'size': pdf_size,
        'url': pdf_url,
        'content': pdf_content,
    }

    es.index(index='pdfs', document=doc)
    print(f"Indexed: {pdf_name}")

# Search for PDFs by query
def search_pdfs(query):
    # Perform a search query on the 'pdfs' index
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["name", "content", "url"]
            }
        }
    }

    response = es.search(index="pdfs", body=body)
    return response['hits']['hits']
