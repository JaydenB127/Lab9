import os
import requests
import zipfile
from urllib.parse import urlparse

# List of URLs to download from
DOWNLOAD_URLS = [
    "http://example.com/file1.zip",  # Replace with actual URLs from main.py
    # ... other URLs ...
]

def create_downloads_directory():
    """Create downloads directory if it doesn't exist"""
    if not os.path.exists('downloads'):
        os.makedirs('downloads')

def get_filename_from_url(url):
    """Extract filename from URL"""
    return os.path.basename(urlparse(url).path)

def download_file(url):
    """Download a file from URL and save it to downloads directory"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise exception for bad status codes
        
        filename = get_filename_from_url(url)
        filepath = os.path.join('downloads', filename)
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return filepath
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def extract_zip(zip_path):
    """Extract CSV from zip file and delete the zip"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall('downloads')
        os.remove(zip_path)  # Delete zip file after extraction
    except zipfile.BadZipFile as e:
        print(f"Error extracting {zip_path}: {e}")

def main():
    # Create downloads directory
    create_downloads_directory()
    
    # Process each URL
    for url in DOWNLOAD_URLS:
        print(f"Processing {url}")
        # Download the file
        zip_path = download_file(url)
        if zip_path:
            # Extract CSV and delete zip
            extract_zip(zip_path)

if __name__ == "__main__":
    main()
