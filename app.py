from flask import Flask, request, render_template
from elasticsearch import Elasticsearch
import os
from crawler.crawler import crawl_and_download
from elasticsearch_index.es_index import create_index, index_pdf, search_pdfs

# Initialize the Flask app
app = Flask(__name__)

# Initialize Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Create index in Elasticsearch if it doesn't exist
create_index()

# Route for the homepage
@app.route('/')
def index():
    return render_template('index.html')  # Display the input form

# Route for scraping process
@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    # Get the URL from the form
    website_url = request.form['url']
    
    # Folder to save downloaded PDFs
    download_folder = './downloaded_pdfs'
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Call the scraping function to start crawling and downloading PDFs
    crawl_and_download(website_url, download_folder)

    # After scraping, index the downloaded PDFs into Elasticsearch
    for pdf_file in os.listdir(download_folder):
        if pdf_file.endswith('.pdf'):
            pdf_path = os.path.join(download_folder, pdf_file)
            pdf_url = website_url + '/' + pdf_file  # Adjust based on the structure of the website
            index_pdf(pdf_path, pdf_url)

    return f"Scraping started for {website_url}. PDFs will be downloaded and indexed."

# Route for searching PDFs
@app.route('/search', methods=['GET', 'POST'])
def search():
    query = request.form.get('query')  # Get the search query from the form
    results = []
    
    if query:
        # Use the search_pdfs function to query Elasticsearch
        results = search_pdfs(query)

    return render_template('search_results.html', results=results)  # Display search results

if __name__ == '__main__':
    app.run(debug=True)
