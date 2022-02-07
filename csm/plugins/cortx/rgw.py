# CORTX-CSM: CORTX Management web and CLI interface.
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.

from typing import Any
from json import loads
from csm.core.services.rgw.s3.utils import CsmRgwConfigurationFactory
from csm.core.data.models.rgw import RgwErrors, RgwError
from csm.common.errors import CsmInternalError
from cortx.utils.log import Log
from csm.core.blogic import const
from cortx.utils.rgwadmin import RGWAdminClient
from cortx.utils.rgwadmin import RGWAdminClientException


class RGWPlugin:

    def __init__(self):
        """
        Initialize RGW plugin
        """
        config = CsmRgwConfigurationFactory.get_rgw_connection_config()
        self._rgw_admin_client = RGWAdminClient(config.auth_user_access_key,
            config.auth_user_secret_key, config.host, config.port)
        Log.info(f"RGW admin uid: {const.ADMIN_UID}")
        self._api_operations = {
            'CREATE_USER': {
                'ENDPOINT': f"/{const.ADMIN_UID}/user",
                'METHOD': "PUT",
                'REQUEST_BODY_SCHEMA': {
                    'uid': 'uid',
                    'display_name': 'display-name',
                    'email': 'email',
                    'key_type': 'key-type',
                    'access_key': 'access-key',
                    'secret_key': 'secret-key',
                    'user_caps': 'user-caps',
                    'generate_key': 'generate-key',
                    'max_buckets': 'max-buckets',
                    'suspended': 'suspended',
                    'tenant': 'tenant'
                },
                'SUCCESS_CODE': 200
            }
        }

    @Log.trace_method(Log.DEBUG)
    async def execute(self, operation, **kwargs) -> Any:
        api_operation = self._api_operations.get(operation)

        request_body = None
        if api_operation['METHOD'] != 'GET':
            request_body = self._build_request(api_operation['REQUEST_BODY_SCHEMA'], **kwargs)

        return await self._process(api_operation, request_body)

    @Log.trace_method(Log.DEBUG)
    def _build_request(self, request_body_schema, **kwargs) -> Any:
        request_body = dict()
        for key, value in request_body_schema.items():
            if kwargs.get(key, None) is not None:
                request_body[value] = kwargs.get(key, None)
        Log.debug(f"RGW Plugin - request body: {request_body}")
        return request_body

    @Log.trace_method(Log.DEBUG)
    async def _process(self, api_operation, request_body) -> Any:
        try:
            (code, body) = await self._rgw_admin_client.signed_http_request(api_operation['METHOD'], api_operation['ENDPOINT'], query_params=request_body)
            response_body = loads(body)
            if code != api_operation['SUCCESS_CODE']:
                return self._create_error(code, response_body)
            return response_body
        except RGWAdminClientException as e:
            Log.error(f'{const.RGW_CLIENT_ERROR_MSG}: {e}')
            raise CsmInternalError(const.RGW_CLIENT_ERROR_MSG)

    def _create_error(self, status: int, body: dict) -> Any:
        """
        Converts a body of a failed query into RgwError object.

        :param status: HTTP Status code.
        :param body: parsed HTTP response (dict) with the error's decription.
        :returns: instance of error.
        """

        Log.error(f"Create error body: {body}")

        rgw_error = RgwError()
        rgw_error.http_status = status
        rgw_error.error_code = RgwErrors[body['Code']]
        rgw_error.error_message = rgw_error.error_code.value

        return rgw_error
