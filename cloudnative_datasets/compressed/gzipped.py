import logging
import os
import re
import io
import subprocess
import tempfile
from typing import BinaryIO

import pandas as pd

from ..cobase import CloudObjectBase
from ..cochunkbase import CloudObjectChunk

logger = logging.getLogger(__name__)

GZTOOL_PATH = '/home/lab144/.local/bin/gztool'
CHUNK_SIZE = 65536

RE_WINDOWS = re.compile(r'#\d+: @ \d+ / \d+ L\d+ \( \d+ @\d+ \)')
RE_NUMS = re.compile(r'\d+')
RE_NLINES = re.compile(r'Number of lines\s+:\s+\d+')


class GZippedText(CloudObjectBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._index_key = self.cloud_object._key + 'i'

    def preprocess(self, object_stream: BinaryIO):
        tmp_file_name = tempfile.mktemp()
        # try:
        #     os.remove(tmp_file_name)
        # except FileNotFoundError:
        #     pass
        index_proc = subprocess.Popen([GZTOOL_PATH, '-i', '-x', '-s', '1', '-I', tmp_file_name],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        chunk = object_stream.read(CHUNK_SIZE)
        while chunk != b"":
            index_proc.stdin.write(chunk)
            chunk = object_stream.read(CHUNK_SIZE)
        object_stream.close()

        stdout, stderr = index_proc.communicate()
        logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))
        assert index_proc.returncode == 0

        with open(tmp_file_name, 'rb') as index_f:
            index_bin = index_f.read()
            output = subprocess.check_output([GZTOOL_PATH, '-ell'], input=index_bin).decode('utf-8')
            logger.debug(output)
        # os.remove(tmp_file_name)
        self.cloud_object.s3_client.put_object(Bucket=self.cloud_object._bucket, Key=self._index_key, Body=index_bin)

        total_lines = RE_NUMS.findall(RE_NLINES.findall(output).pop()).pop()

        lines = []
        for f in RE_WINDOWS.finditer(output):
            nums = [int(n) for n in RE_NUMS.findall(f.group())]
            lines.append(nums)

        df = pd.DataFrame(lines, columns=['window', 'compressed_byte', 'uncompressed_byte',
                                          'line_number', 'window_size', 'window_offset'])
        df.set_index(['window'], inplace=True)

        out_stream = io.BytesIO()
        df.to_parquet(out_stream, engine='pyarrow')
        df.to_csv('test.csv')
        return out_stream.getvalue(), {'total_lines': total_lines}

    def partition_chunk_lines(self, lines_per_chunk):
        x = self.cloud_object.get_attribute('total_lines')
        # TODO
        # make use of index to get the ranges of the chunks partitioned by number of lines per chunk
        # return list of range tuples [(chunk0-range0, chunk-0range1), ...]
        pass

    def partition_n_chunks(self, n_chunks):
        x = self.cloud_object.get_attribute('total_lines')
        pass

    def get_line_range(self, line0, line1):
        meta_obj = self.cloud_object.get_meta_obj()
        df = pd.read_parquet(meta_obj)
        res = df.loc[(df['line_number'] >= line0) & (df['line_number'] <= line1)]
        window0 = res.head(1)['compressed_byte'].values[0].item()
        window1 = df.loc[res.tail(1).index + 1]['compressed_byte'].values[0].item()

        return GZipLineIterator(range0=window0, range1=window1, line0=line0, line1=line1,
                                cloud_object=self.cloud_object, child=self)


class GZipLineIterator(CloudObjectChunk):
    def __init__(self, range0, range1, line0, line1, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.range0 = range0
        self.range1 = range1
        self.line0 = line0
        self.line1 = line1

        self._body = None

    def setup(self):
        tmp_index_file = tempfile.mktemp()
        res = self.cloud_object.s3_client.get_object(Bucket=self.cloud_object._bucket, Key=self.child._index_key)
        with open(tmp_index_file, 'wb') as index_tmp:
            index_tmp.write(res['Body'].read())

        res = self.cloud_object.s3_client.get_object(Bucket=self.cloud_object._bucket, Key=self.cloud_object._key,
                                                     Range=f'{self.range0 - 1}-{self.range1}')
        self._body = res['Body']

        tmp_chunk_file = tempfile.mktemp()
        with open(tmp_chunk_file, 'wb') as chunk_tmp:
            chunk_tmp.write(self._body.read())

        index_proc = subprocess.Popen([GZTOOL_PATH, '-I', tmp_index_file, '-n', str(self.range0), tmp_chunk_file])

        stdout, stderr = index_proc.communicate()
        # logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))
        assert index_proc.returncode == 0

        return stdout