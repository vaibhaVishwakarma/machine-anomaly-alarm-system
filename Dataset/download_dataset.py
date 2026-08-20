"""
Dataset Download Script
Dataset: Industrial Water Pump Anomaly Detection (Audio)
Source:  https://www.kaggle.com/datasets/vuppalaadithyasairam/anomaly-detection-in-water-pump-using-audio-data

Requirements:
    pip install kagglehub

Usage:
    python Dataset/download_dataset.py
"""

import kagglehub
import shutil
import os

# Download latest version to kagglehub cache
path = kagglehub.dataset_download(
    "vuppalaadithyasairam/anomaly-detection-in-water-pump-using-audio-data"
)

print("Downloaded to cache:", path)

# Extract/copy into ./Dataset (same folder as this script)
dest = os.path.dirname(os.path.abspath(__file__))

for item in os.listdir(path):
    src = os.path.join(path, item)
    dst = os.path.join(dest, item)
    if os.path.isdir(src):
        if os.path.exists(dst):
            print(f"Already exists, skipping: {dst}")
        else:
            print(f"Copying {item} -> {dst}")
            shutil.copytree(src, dst)
    else:
        print(f"Copying {item} -> {dst}")
        shutil.copy2(src, dst)

print("\nDataset ready at:", dest)
