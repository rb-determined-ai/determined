import logging
import os
import shutil
import urllib.parse
from typing import Any, Dict

import filelock
import requests

from torchvision import datasets, transforms


def get_dataset(data_dir: str, train: bool) -> Any:
    return datasets.MNIST(
        data_dir,
        train=train,
        transform=transforms.Compose(
            [
                transforms.ToTensor(),
                # These are the precomputed mean and standard deviation of the
                # MNIST data; this normalizes the data to have zero mean and unit
                # standard deviation.
                transforms.Normalize((0.1307,), (0.3081,)),
            ]
        ),
    )


def download_dataset(url: str, download_dir: str) -> str:
    # We download from s3 since torchvision's default MNIST mirror is not super reliable.
    url_path = urllib.parse.urlparse(url).path
    basename = url_path.rsplit("/", 1)[1]

    # Use a file lock so only one worker on each node does the download.
    lockpath = os.path.join(download_dir, "download.lock")
    mnist_dir = os.path.join(download_dir, "MNIST")
    tarpath = os.path.join(download_dir, basename)
    os.makedirs(download_dir, exist_ok=True)
    with filelock.FileLock(lockpath):
        if not os.path.exists(mnist_dir):
            logging.info(f"Downloading {url}...")
            r = requests.get(url, stream=True)
            with open(tarpath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logging.info(f"Extracting {url}...")
            os.mkdir(mnist_dir)
            shutil.unpack_archive(tarpath, mnist_dir)
