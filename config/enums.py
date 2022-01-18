'''
A central location for enums used throughout the program
'''

from enum import Enum

class SchemaKey(Enum):
    '''
    The complete list of acceptable values for schema keys
    '''
    TYPE = 'type'
    FORMAT = 'format'
    NULLABLE = 'nullable'
    CONTENTS = 'contents'


class SchemaType(Enum):
    '''
    The complete list of acceptable values for schema data types
    '''
    INTEGER = 'integer'
    TIMESTAMP = 'timestamp'
    DATE = 'date'
    JSONB = 'jsonb'
    STRING = 'string'


class DatabaseSchemaField(Enum):
    '''
    Field names when inserting values into the Postgres table
    '''
    EVENT_TYPE = 'event_type'
    EVENT_TIME = 'event_time'
    DATA = 'data'
    PROCESSING_DATE = 'processing_date'
