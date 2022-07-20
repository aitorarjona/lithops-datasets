import asyncio
import logging
import re
import io
import tempfile

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

    async def preprocess(self, get_future):

        pipe = await asyncio.subprocess.create_subprocess_shell(f'{GZTOOL_PATH} -i -x -s 1',
                                                                stdin=asyncio.subprocess.PIPE,
                                                                stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE)

        await get_future
        get_response = get_future.result()
        body_stream = get_response['Body']
        result_buffer = io.BytesIO()

        async def _async_writer():
            logger.debug('start writing input')
            input_chunk = await body_stream.read(CHUNK_SIZE)
            while input_chunk != b"":
                pipe.stdin.write(input_chunk)
                input_chunk = await body_stream.read(CHUNK_SIZE)
            logger.debug('done writing input')

        async def _async_reader():
            logger.debug('start reading output')
            output_chunk = await pipe.stdout.read()
            while output_chunk != b"":
                result_buffer.write(output_chunk)
                output_chunk = await pipe.stdout.read()
            logger.debug('done reading output')

        async def _async_err():
            logger.debug('start reading err')
            output_chunk = await pipe.stderr.read()
            while output_chunk != b"":
                print(output_chunk)
            logger.debug('done reading err')

        await asyncio.gather(_async_writer(), _async_reader(), _async_err())
        # await asyncio.gather(_async_writer())

        stdout, stderr = await pipe.communicate()
        # logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))
        assert pipe.returncode == 0

        output = subprocess.check_output([GZTOOL_PATH, '-ell'], input=result_buffer.getvalue()).decode('utf-8')
        logger.debug(output)
        await self.cloud_object._s3.put_object(Bucket=self.cloud_object._meta_bucket, Key=self._index_key,
                                               Body=result_buffer.getvalue())

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
        meta_buff = io.BytesIO(meta_obj.read())
        meta_buff.seek(0)
        df = pd.read_parquet(meta_buff)
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

        # Get index and store it to temp file
        res = self.cloud_object.s3_client.get_object(Bucket=self.cloud_object._meta_bucket, Key=self.child._index_key)
        with open(tmp_index_file, 'wb') as index_tmp:
            index_tmp.write(res['Body'].read())

        res = self.cloud_object.s3_client.get_object(Bucket=self.cloud_object._obj_bucket, Key=self.cloud_object._key,
                                                     Range=f'bytes={self.range0 - 1}-{self.range1 - 1}')
        self._body = res['Body']

        tmp_chunk_file = tempfile.mktemp()
        with open(tmp_chunk_file, 'wb') as chunk_tmp:
            chunk_tmp.write(self._body.read())

        cmd = [GZTOOL_PATH, '-I', tmp_index_file, '-n', str(self.range0), '-L', str(self.line0), tmp_chunk_file]
        index_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = index_proc.communicate()
        # logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))

        return stdout
