import requests
import os
import zipfile
from urllib.parse import urlparse

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",  # Fixed typo in year
]

def create_downloads_directory():
    """Create downloads directory if it doesn't exist"""
    if not os.path.exists('downloads'):
        os.makedirs('downloads')

def get_filename_from_url(url):
    """Extract filename from URL"""
    return os.path.basename(urlparse(url).path)

def download_and_process_file(url):
    """Download a file from URL, save it, extract CSV, and delete the zip"""
    try:
        # Get filename from URL
        filename = get_filename_from_url(url)
        filepath = os.path.join('downloads', filename)
        
        print(f"Downloading {filename}...")
        
        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Save the zip file
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"Extracting {filename}...")
        
        # Extract the zip file
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall('downloads')
            
            # Delete the zip file after successful extraction
            os.remove(filepath)
            print(f"Successfully processed {filename}")
            
        except zipfile.BadZipFile:
            print(f"Error: {filename} is not a valid zip file")
            if os.path.exists(filepath):
                os.remove(filepath)
                
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
    except Exception as e:
        print(f"Unexpected error processing {url}: {e}")

def main():
    # Create downloads directory
    create_downloads_directory()
    
    # Process each URL
    for uri in download_uris:
        # Remove any extra whitespace and quotes
        uri = uri.strip().strip('`').strip()
        download_and_process_file(uri)

if __name__ == "__main__":
    main()
