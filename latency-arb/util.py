import os
import tarfile
import gzip
import pandas as pd

from settings import *


def extract_all_tar(TAR_FILE_PATH, GZ_DIR):
    with tarfile.open(TAR_FILE_PATH, 'r') as tar:
        tar.extractall(GZ_DIR)

def extract_all_gz(GZ_DIR):
    data = {}
    for file in os.listdir(GZ_DIR):
        if file.endswith('.gz'):
            full_path = os.path.join(GZ_DIR, file)
            with gzip.open(full_path, 'rt') as f:
                data[file] = pd.read_csv(f)
    return data