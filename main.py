from signal_processing import load_all as signal
from util import *

# extract_all_tar(TAR_FILE_PATH, GZ_DIR)
data = extract_all_gz(GZ_DIR)
signal_dict = signal(data, threshold=50, latency=10, transaction_fee=50)
print(signal_dict)