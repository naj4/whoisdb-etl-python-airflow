from extract import download_files
from crud_postgres import db_setup, load_files_to_db
from constants import URL, USERNAME, PASSWORD, DESTINATION_DIR

def extract_files():
    filenames = db_setup()
    if filenames:  # Check if filenames is not empty
        download_files(URL, USERNAME, PASSWORD, filenames, DESTINATION_DIR)
    else:
        print("No files to download.")
    # unzip_gz_files()
    # load_files_to_db()