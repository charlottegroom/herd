'''Configuration Schema Validation'''

from marshmallow import Schema, post_load, validate
from marshmallow.exceptions import ValidationError
from marshmallow.fields import (
    Integer,
    Nested,
    String
)
from marshmallow_oneofschema import OneOfSchema
import os


class SourceSchema(Schema):
    '''Custom schema per source'''
    class Meta:
        unknown = 'INCLUDE'


class SinkBase(Schema):
    name = String(required=True)
    chunksize = Integer(default=100, missing=100)
    mode = String(
        validate=validate.OneOf(['replace', 'append']),
        default='replace',
        missing='replace'
    )


class CSVSinkSchema(SinkBase):
    '''Schema for CSV sink'''
    type = String(required=True)


class PostgresSinkSchema(SinkBase):
    '''Schema for Postgres sink'''
    type = String(required=True)

    @post_load
    def ensure_env(self, data, **kwargs):
        uri = os.environ.get('POSTGRESQL')
        if not uri:
            raise ValidationError('POSTGRESQL Environment variable not set!')
        return data


class SinkSchema(OneOfSchema):
    '''Schema for sink types'''
    type_field_remove = False
    type_schemas = {
        'csv': CSVSinkSchema,
        'postgres': PostgresSinkSchema,
    }


class IngestSchema(Schema):
    '''Schema for Ingest class config'''
    source = Nested(SourceSchema, required=True)
    sink = Nested(SinkSchema)
