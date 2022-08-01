from minio import Minio
from minio.error import S3Error, ServerError
from typing import Any

from csm.core.blogic import const
from csm.common.payload import Json
from cortx.utils.log import Log
from csm.common.errors import CsmInternalError


class HaloMinio():
    __endpoint = "ssc-vm-g2-rhev4-3394.colo.seagate.com:30869"
    __access_key = "demouser1"
    __secret_key = "demouser1"
    def __init__(self):
        self.client = Minio(HaloMinio.__endpoint,
            HaloMinio.__access_key,
            HaloMinio.__secret_key,
            secure=False)
        self._api_operations = Json(const.MINIO_OPERATIONS_MAPPING_SCHEMA).load()
    
    async def execute(self, operation, **kwargs) -> Any:
        api_operation = self._api_operations.get(operation)
        args_list = self._build_request(api_operation['REQUEST_BODY_SCHEMA'], **kwargs)
        return await self._process(api_operation, args_list)

    def _build_request(self, request_body_schema, **kwargs) -> Any:
        args_list = list()
        for key, value in request_body_schema.items():
            args_list.append(kwargs.get(key, None))
        return args_list

    async def _process(self, api_operation, args_list) -> Any:

            try:
                client_method = getattr(self.client, api_operation['ENDPOINT'])
                response = client_method(*args_list)
                return response if response else {}
            except S3Error as err:
                return {
                    "error_code":"halo",
                    "message_id":err.code,
                    "message":err.message,
                    "http_response":err.response.status
                }
            except ServerError as err:
                return {
                    "error_code":"halo",
                    "http_response":err.status_code
                }
            except Exception as e:
                Log.error(f'{const.UNKNOWN_ERROR}: {e}')
                raise CsmInternalError(const.S3_CLIENT_ERROR_MSG)

