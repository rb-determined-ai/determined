import logging
import os
import tempfile
from typing import Callable, Optional, Union

from hdfs.client import InsecureClient

from determined.common import storage, util


class HDFSStorageManager(storage.CloudStorageManager):
    """
    Store and load checkpoints from HDFS.
    """

    def __init__(
        self,
        hdfs_url: str,
        hdfs_path: str,
        user: Optional[str] = None,
        temp_dir: Optional[str] = None,
    ) -> None:
        super().__init__(temp_dir if temp_dir is not None else tempfile.gettempdir())

        self.hdfs_url = hdfs_url
        self.hdfs_path = hdfs_path
        self.user = user

        self.client = InsecureClient(self.hdfs_url, root=self.hdfs_path, user=self.user)

    @util.preserve_random_state
    def upload(self, src: Union[str, os.PathLike], dst: str) -> None:
        src = os.fspath(src)
        logging.info(f"Uploading to HDFS: {dst}")
        self.client.upload(dst, src)

    @util.preserve_random_state
    def download(
        self,
        src: str,
        dst: Union[str, os.PathLike],
        selector: Optional[Callable[[str], bool]] = None,
    ) -> None:
        dst = os.fspath(dst)
        if selector is not None:
            raise NotImplementedError(
                "hdfs checkpoint storage does not currently support sharded downloads"
            )
        logging.info(f"Downloading {src} from HDFS")
        self.client.download(src, dst, overwrite=True)

    @util.preserve_random_state
    def delete(self, tgt: str) -> None:
        logging.info(f"Deleting {tgt} from HDFS")
        self.client.delete(tgt, recursive=True)
