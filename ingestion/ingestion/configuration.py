'''Configuration Schema Validation'''

from marshmallow import Schema
from marshmallow.fields import (
    String
)
from marshmallow_oneofschema import OneOfSchema


class SourceSchema(Schema):
    '''Custom schema per source'''
    class Meta:
        unknown = 'INCLUDE'


class CSVSinkSchema(Schema):
    '''Schema for CSV sink'''
    type = String(required=True)


class SinkSchema(OneOfSchema):
    '''Schema for sink types'''
    type_field_remove = False
    type_schemas = {'csv': CSVSinkSchema}
    name = String(required=True)