import logging
import asyncio

from cloudnative_datasets import CloudObject
from cloudnative_datasets.genomics import FASTQGZip

logging.basicConfig(level=logging.DEBUG)


async def main():
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://127.0.0.1:9000',
        's3_config_kwargs': {
            'signature_version': 's3v4'
        }
    }

    co = CloudObject.new_from_s3(FASTQGZip, 's3://genomics/1c-12S_S96_L001_R1_001.fastq.gz', s3_config=config)
    await co.fetch()

    is_staged = await co.is_staged()
    print(is_staged)
    # if not is_staged:
    #     await co.preprocess()
    await co.preprocess()

    total_lines = co.get_attribute('total_lines')
    print(total_lines)

    it = co.call(FASTQGZip.get_line_range, 1, 125000)
    it.setup()


if __name__ == '__main__':
    asyncio.run(main())
