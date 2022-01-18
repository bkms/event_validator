'''
Custom Exceptions classes
'''

class InvalidJSONLineException(Exception):
    '''
    Any JSON line that throws this exception or its subclass exceptions will not be 
    ingested into the Postgres landing table, instead it is logged and/or dumped 
    into a generic error bucket/table/file.
    '''


class InvalidFieldException(InvalidJSONLineException):
    '''
    This exception is raised when a given event field does not exist in the
    schema definition
    '''


class MissingMandatoryFieldValueException(InvalidJSONLineException):
    '''
    This exception is raised when an event line has a key but no value
    for a non-nullable field
    '''
