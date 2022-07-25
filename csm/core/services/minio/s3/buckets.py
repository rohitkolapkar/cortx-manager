from minio.error import S3Error, ServerError
from csm.core.blogic import const
from csm.plugins.cortx.minio_s3 import HaloMinio
from csm.common.services import ApplicationService


class BucketsService(ApplicationService):

    def __init__(self):
        self._plugin =  HaloMinio()

    async def list_buckets(self):
        request_body = {}
        return await self._plugin.execute('LIST_BUCKETS', **request_body)

    async def make_bucket(self, **request_body):
        return await self._plugin.execute('MAKE_BUCKET', **request_body)
