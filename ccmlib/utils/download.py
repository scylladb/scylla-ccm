import os
import logging
import shutil
import urllib.parse

import tqdm
import requests
from boto3.session import Session
from boto3.s3.transfer import S3Transfer, TransferConfig
import botocore
import botocore.client


def url_filename(url: str):
     return url.rsplit('/', maxsplit=1)[-1]


# copy from from https://stackoverflow.com/a/29967714/459189
def copyfileobj(fsrc, fdst, callback, length=0):
    try:
        # check for optimisation opportunity
        if "b" in fsrc.mode and "b" in fdst.mode and fsrc.readinto:
            return _copyfileobj_readinto(fsrc, fdst, callback, length)
    except AttributeError:
        # one or both file objects do not support a .mode or .readinto attribute
        pass

    if not length:
        length = shutil.COPY_BUFSIZE

    fsrc_read = fsrc.read
    fdst_write = fdst.write

    copied = 0
    while True:
        buf = fsrc_read(length)
        if not buf:
            break
        fdst_write(buf)
        copied += len(buf)
        callback(copied)

# differs from shutil.COPY_BUFSIZE on platforms != Windows
READINTO_BUFSIZE = 1024 * 1024


def _copyfileobj_readinto(fsrc, fdst, callback, length=0):
    """readinto()/memoryview() based variant of copyfileobj().
    *fsrc* must support readinto() method and both files must be
    open in binary mode.
    """
    fsrc_readinto = fsrc.readinto
    fdst_write = fdst.write

    if not length:
        try:
            file_size = os.stat(fsrc.fileno()).st_size
        except OSError:
            file_size = READINTO_BUFSIZE
        length = min(file_size, READINTO_BUFSIZE)

    copied = 0
    with memoryview(bytearray(length)) as mv:
        while True:
            n = fsrc_readinto(mv)
            if not n:
                break
            elif n < length:
                with mv[:n] as smv:
                    fdst.write(smv)
            else:
                fdst_write(mv)
            copied += n
            callback(copied)


class TqdmUpTo(tqdm.tqdm):
    """Alternative Class-based version of the above.
    Provides `update_to(n)` which uses `tqdm.update(delta_n)`.
    Inspired by [twine#242](https://github.com/pypa/twine/pull/242),
    [here](https://github.com/pypa/twine/commit/42e55e06).
    """

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)  # also sets self.n = b * bsize


def download_file(url: str, target_path: str = None, verbose: bool = False):
    """
    Download url via http

    :param url: url to download
    :param target_path: full path to save the download
    :param verbose: if True, there would be a progress bar
    :return:
    """

    chunk_size = 2 * 1024 * 1024
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()

        total = int(resp.headers.get('content-length', 0))

        with open(target_path, 'wb') as f, \
                TqdmUpTo(desc=f'HTTP download: {url_filename(url)}', total=total, unit='B', unit_scale=1, position=0,
                         bar_format='{desc:<10}{percentage:3.0f}%|{bar:10}{r_bar}',
                         disable=None if verbose else True) as bar:
            if verbose:
                def clbk(size):
                    bar.update_to(size)
                copyfileobj(resp.raw, f, length=chunk_size, callback=clbk)
            else:
                shutil.copyfileobj(resp.raw, f, length=chunk_size)

    return target_path


def download_version_from_s3(url: str, target_path: str, verbose=False):
    """
    Download url from s3 via boto3

    :param url: url to download
    :param target_path: full path to save the download
    :param verbose: if True, there would be a progress bar
    :return:
    """

    target_path = str(target_path)
    parts = urllib.parse.urlparse(url)
    _, bucket_name, download_path = parts.path.split('/', maxsplit=2)
    s3_client = Session().client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    try:
        metadata = s3_client.head_object(Bucket=bucket_name, Key=download_path)
    except botocore.client.ClientError as ex:
        if 'Not Found' in str(ex):
            logging.warning(f"url: '{url}' wasn't found on S3")
            logging.warning(f"download might be very slow")
            return None
        else:
            raise

    total = metadata['ContentLength']
    with tqdm.tqdm(desc=f'S3 download: {url_filename(url)}', total=total,
                   unit='B', unit_scale=1, position=0,
                   bar_format='{desc:<10}{percentage:3.0f}%|{bar:10}{r_bar}', disable=None if verbose else True) as progress:
        transfer = S3Transfer(s3_client, config=TransferConfig(max_concurrency=20))
        transfer.download_file(bucket_name, download_path, target_path, callback=progress.update)

    return target_path
