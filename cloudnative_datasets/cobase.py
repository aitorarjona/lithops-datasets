import asyncio
import inspect
import logging
from typing import Tuple, Dict, BinaryIO

import botocore
from aiobotocore.session import get_session

from .util import split_s3_path

logger = logging.getLogger(__name__)


class CloudObjectBase:
    def __init__(self, cloud_object):
        self.cloud_object: CloudObject = cloud_object

    def preprocess(self, object_stream: BinaryIO) -> Tuple[bytes, Dict[str, str]]:
        raise NotImplementedError()


class CloudObject:
    def __init__(self, cloud_object_class, s3_path, s3_config=None):
        self._obj_meta = None
        self._meta_meta = None
        self._s3_path = s3_path
        self._cls = cloud_object_class
        self._obj_attrs = {}
        self._s3_config = s3_config or {}
        self._s3 = None

        self._obj_bucket, self._key = split_s3_path(s3_path)
        self._meta_bucket = self._obj_bucket + '.meta'

        logger.debug(f'{self._obj_bucket=},{self._meta_bucket=},{self._key=}')

        self._child = cloud_object_class(self)

    @property
    def path(self):
        return self._s3_path

    @classmethod
    def new_from_s3(cls, cloud_object_class, s3_path, s3_config=None):
        co_instance = cls(cloud_object_class, s3_path, s3_config)
        return co_instance

    @classmethod
    def new_from_file(cls, cloud_object_class, file_path, cloud_path, s3_config=None):
        co_instance = cls(cloud_object_class, cloud_path, s3_config)

        if co_instance.exists():
            raise Exception('Object already exists')

        bucket, key = split_s3_path(cloud_path)

        co_instance.s3_client.upload_file(Filename=file_path, Bucket=bucket, Key=key)

    async def _setup(self):
        self._s3_context = get_session().create_client(
            's3',
            aws_access_key_id=self._s3_config.get('aws_access_key_id'),
            aws_secret_access_key=self._s3_config.get('aws_secret_access_key'),
            region_name=self._s3_config.get('region_name'),
            endpoint_url=self._s3_config.get('endpoint_url'),
            config=botocore.client.Config(**self._s3_config.get('s3_config_kwargs', {}))
        )
        self._s3 = await self._s3_context.__aenter__()

    def _update_attrs(self):
        print(self._meta_meta)
        self._attributes = {key: value for key, value in self._meta_meta['Metadata'].items()}

    def exists(self):
        if not self._obj_meta:
            self.fetch()
        return bool(self._obj_meta)

    async def is_staged(self):
        try:
            await self._s3.head_object(Bucket=self._meta_bucket, Key=self._key)
            return True
        except botocore.exceptions.ClientError as e:
            logger.debug(e.response)
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    def get_attribute(self, key):
        return self._obj_attrs[key]

    async def fetch(self):
        if self._s3 is None:
            await self._setup()
        if not self._obj_meta:
            logger.debug('fetching object head')
            try:
                head_res = await self._s3.head_object(Bucket=self._obj_bucket, Key=self._key)
                del head_res['ResponseMetadata']
                self._obj_meta = head_res
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    self._obj_meta = None
                else:
                    raise e
        if not self._meta_meta:
            logger.debug('fetching meta head')
            try:
                head_res = await self._s3.head_object(Bucket=self._meta_bucket, Key=self._key)
                del head_res['ResponseMetadata']
                self._meta_meta = head_res
                if 'Metadata' in head_res:
                    self._obj_attrs.update(head_res['Metadata'])
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    self._meta_meta = None
                else:
                    raise e
        return self._obj_meta, self._meta_meta

    async def preprocess(self):
        get_fut = asyncio.ensure_future(self._s3.get_object(Bucket=self._obj_bucket, Key=self._key))
        logger.debug(get_fut)
        body, meta = await self._child.preprocess(get_future=get_fut)
        put_res = self._s3.put_object(
            Body=body,
            Bucket=self._meta_bucket,
            Key=self._key,
            Metadata=meta
        )
        logger.debug(put_res)
        self._obj_attrs.update(meta)

    def get_meta_obj(self):
        get_res = self.s3_client.get_object(Bucket=self._meta_bucket, Key=self._key)
        return get_res['Body']

    def call(self, f, *args, **kwargs):
        if isinstance(f, str):
            func_name = f
        elif inspect.ismethod(f) or inspect.isfunction(f):
            func_name = f.__name__
        else:
            raise Exception(f)

        attr = getattr(self._child, func_name)
        return attr.__call__(*args, **kwargs)
