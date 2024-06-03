from extract import download_files
from unzip import unzip_gz_files
from crud_postgres import db_setup, load_files_to_db

# Constants
URL = 'https://domainwhoisdatabase.com/whois_database/sample/gtlds/v47/sample/simple/'
USERNAME = 'sample'
PASSWORD = 'sample999!'
DESTINATION_DIR = r'C:\WhoIsDbIntegration\Destination'

def run_whois_etl():
    filenames = db_setup()
    download_files(URL, USERNAME, PASSWORD, filenames, DESTINATION_DIR)
    unzip_gz_files(DESTINATION_DIR)
    load_files_to_db(DESTINATION_DIR)