import gzip
import os
import shutil

# Unzip all .gz files in specifird directory
def unzip_gz_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.gz'):
            file_path = os.path.join(directory, filename)
            unzipped_file_path = os.path.join(directory, filename[:-3])

            with gzip.open(file_path, "rb") as f_in:
                with open(unzipped_file_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                
            print(f"Unzipped: {file_path} to {unzipped_file_path}")

