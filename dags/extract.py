import os
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup

# Retrieve files listed in file_names from specified URL and download them to destination dir
def download_files(base_url, username, password, file_names, destination_dir):
    # Create destination dir if not exists
    os.makedirs(destination_dir, exist_ok=True)
    
    # create an authentication object using the username and password
    auth = HTTPBasicAuth(username, password)

    response = requests.get(base_url, auth=auth)
    if response.status_code == 200: # if request successful
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a'):
            file_url = link.get('href')
            if file_url.endswith('.gz'):
                if file_url in file_names:                
                    full_url = base_url + file_url # Construct full url
                    destination_path = os.path.join(destination_dir, file_url)
                    download_file(full_url, auth, destination_path)

    else:
        print(f"Failed to retrieve file list from: {base_url}. Status code: {response.status_code}")


# Downloads a file from the specified url and saves it to the destination path
def download_file(url, auth, destination):
    response = requests.get(url, auth=auth)
    if response.status_code == 200:
        with open(destination, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {url} to {destination}")
    else:
        print(f"Failed to download: {url}. Status code: {response.status_code}")