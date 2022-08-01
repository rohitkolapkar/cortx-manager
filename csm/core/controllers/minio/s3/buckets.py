import json
from marshmallow import fields, ValidationError
from cortx.utils.log import Log
from csm.common.errors import InvalidRequest
from csm.common.permission_names import Resource, Action
from csm.core.blogic import const
from csm.core.controllers.view import CsmView, CsmAuth, CsmResponse
from csm.core.controllers.validators import ValidationErrorFormatter, ValidateSchema
from csm.common.errors import ServiceError
from csm.core.services.minio.s3.buckets import BucketsService

class MakeBucketSchema(ValidateSchema):
    bucket_name  = fields.Str(data_key=const.BUCKET_NAME, required=True)
    location = fields.Str(data_key=const.LOCATION, allow_none=False)
    object_lock = fields.Bool(data_key=const.OBJECT_LOCK, allow_none=False)

@CsmView._app_routes.view('/api/v2/buckets')
class BucketsView(CsmView):
    def __init__(self, request):
        super(BucketsView, self).__init__(request)
        self._service = BucketsService()

    @CsmAuth.permissions({Resource.S3BUCKETS: {Action.LIST}})
    async def get(self):
        Log.info(f"Handling list buckets GET request"
                 f" user_id: {self.request.session.credentials.user_id}")
        response = await self._service.list_buckets()
        bucket_list = list()
        for bucket in response:
            bucket_list.append({"id":bucket})
        return {"buckets":bucket_list}

    @CsmAuth.permissions({Resource.S3BUCKETS: {Action.CREATE}})
    async def post(self):
        Log.info(f"Handling make bucket POST request"
                 f" user_id: {self.request.session.credentials.user_id}")
        try:
            schema = MakeBucketSchema()
            request_body = schema.load(await self.request.json(), unknown='EXCLUDE')
        except json.decoder.JSONDecodeError:
            raise InvalidRequest(const.JSON_ERROR)
        except ValidationError as val_err:
            raise InvalidRequest(f"{ValidationErrorFormatter.format(val_err)}")
        response = await self._service.make_bucket(**request_body)
        return response