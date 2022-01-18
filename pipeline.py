'''
My solution to the coding challenge.

The module first creates/truncates a Postgres table, then lazily reads a newline JSON
file as input. Each line is validated to ensure it matches the data type specified in
the `config/schema.json` file. The valid lines are then inserted into the Postgres
table and errors written to standard out and log file.

There as several ways in which this could be optimised, I've put some of my comments
in the code and I'm happy to discuss further how I would go about optimising it.
'''


import json

from io import TextIOWrapper
from datetime import datetime
from psycopg2 import OperationalError, DataError, DatabaseError
from psycopg2.extensions import cursor
from config import constants, custom_logging, database_conn
from config.enums import SchemaKey, SchemaType, DatabaseSchemaField
from config.exceptions import InvalidFieldException, MissingMandatoryFieldValueException


logger = custom_logging.get_logger("event_validation_logger")


def yield_json_line(input_file_wrapper: TextIOWrapper) -> dict:
    '''
    A lazy loading generator to ensure large files are read line by line to save memory.

    Parameters:
    ----------
        input_file_wrapper (TextIOWrapper): The IO wrapper object of the file to read

    Yields:
    --------
        dict: the line of JSON converted into a Python key value dict
    '''
    while True:
        data = input_file_wrapper.readline()
        if not data:
            break
        try:
            yield json.loads(data)
        except json.JSONDecodeError:
            logger.error(f"Skipping this input line as it is not valid JSON and can't be decoded into Python: {data}")
            continue


class Validator():
    '''
    A wrapper class to contain the logic of validation, ingestion and monitoring.

    I'm aware this class is not very SOLID, if I spent some more time on it I'd make it nicer.
    '''

    def __init__(self, data_filename: str = constants.FILE_NAME, schema_filename: str = constants.SCHEMA_FILE):
        with open(schema_filename, 'rb') as schema_file:
            nested_schema = [json.loads(line) for line in schema_file]
            self.schema = {
                field_name: field_value for field in nested_schema for field_name, field_value in field.items()}

        self.filename = data_filename
        self.database_connection = database_conn.get_database_connection()


    def process_file(self) -> None:
        '''
        Given the lazy loading loop, the constituent parts of this workflow (read,
        validate and write to Postgres) are all handled within this one method
        '''
        try:
            with open(self.filename, 'rb') as large_file:
                insertion_cursor = self.database_connection.cursor()               
                for json_line in yield_json_line(large_file):
                    is_valid_line = self.validate_schema(json_line)
                    if is_valid_line:
                        self.insert_into_postgres_table(json_line, insertion_cursor)
        except (FileNotFoundError, IOError) as ex:
            logger.error(f"Execution terminated; input data file not found or cannot \
be opened: {self.filename} \n {ex}")
        finally:
            insertion_cursor.close()
            self.database_connection.close()


    def validate_schema(self, line: dict, nested_contents: list = None) -> bool:
        '''
        This only checks that the event types match those in the schema definition and
        whether mandatory field values are missing. I could also check duplicates, etc
        but I didn't implement them here given the time constraint.

        Parameters:
        ----------
            line (dict): The dictionary containing one line from the newline JSON input file
            nested_contents (list): A list of the string field names that are valid keys
                inside the content of the nested object in question

        Returns:
        --------
            bool: Returns True if the line was determined to be valid and False otherwise
        '''
        try:
            for field, value in line.items():

                schema_spec = self.schema.get(field)

                if nested_contents and field not in nested_contents:
                    raise InvalidFieldException(
                        f"Field '{field}' not defined in nested schema: {nested_contents}")

                if not schema_spec:
                    raise InvalidFieldException(
                        f"This field does not exist in Schema file: {field}")

                if not schema_spec.get(SchemaKey.NULLABLE.value, True) and not value:
                    raise MissingMandatoryFieldValueException(
                        f"This mandatory field requires a value, which is missing in this line: {field}")

                schema_type = schema_spec.get(SchemaKey.TYPE.value)

                match schema_type:
                    case SchemaType.INTEGER.value:
                        int(value)

                    case (SchemaType.TIMESTAMP.value | SchemaType.DATE.value):
                        schema_format = schema_spec.get(SchemaKey.FORMAT.value)
                        if not schema_format:
                            raise Exception
                        datetime.strptime(value, schema_format)

                    case SchemaType.STRING.value:
                        str(value)

                    case SchemaType.JSONB.value:
                        # This is recursive in case we want to extend schema depth later
                        self.validate_schema(
                            value, schema_spec.get(SchemaKey.CONTENTS.value))
            
            return True

        except (ValueError, TypeError, 
            MissingMandatoryFieldValueException, InvalidFieldException) as ex:
            if nested_contents:
                raise
            logger.warning(
                f'Schema data type mismatch on field: {field}. Entire invalid line omitted \
from table and written to error file.\n Line: {line}\n Exception: {ex}\n\n')
            return False

    def insert_into_postgres_table(self, line: dict, insertion_cursor: cursor) -> None:
        '''
        It would definitely be much faster to batch insert, which is what I would do as an
        optimization to this method.

        Parameters:
        ----------
            line (dict): The dictionary containing one line from the newline JSON input file
            cursor (PG ): A list of the string field names that are valid keys
                inside the content of the nested object in question

        Returns:
        --------
            bool: Returns True if the line was determined to be valid and False otherwise

        '''
        pg_event_type_int = line.get(DatabaseSchemaField.EVENT_TYPE.value)
        pg_event_time_str = line.get(DatabaseSchemaField.EVENT_TIME.value)
        pg_data_str = json.dumps(line.get(DatabaseSchemaField.DATA.value))
        pg_processing_date_str = line.get(DatabaseSchemaField.PROCESSING_DATE.value)

        sql = f"INSERT INTO {constants.POSTGRES_TABLE} VALUES ({pg_event_type_int}, '{pg_event_time_str}', '{pg_data_str}', '{pg_processing_date_str}');"

        try:
            insertion_cursor.execute(sql)
        except OperationalError as ex:
            logger.error(f"Line failed to insert due to OperationalError (https://www.psycopg.org/docs/errors.html): {line} \n{ex}")
        except DataError as ex:
            logger.error(f"Line failed to insert due to DataError (https://www.psycopg.org/docs/errors.html): {line} \n{ex}")
        except DatabaseError as ex:
            logger.error(f"Line failed to insert due to DatabaseError (https://www.psycopg.org/docs/errors.html): {line} \n{ex}")
            

def pg_setup():
    '''
    If I had some more time, I would put this logic in the Dockerfile instead.
    '''
    connection = database_conn.get_database_connection()
    setup_cursor = connection.cursor()
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {constants.POSTGRES_TABLE} (event_type integer NOT NULL, event_time timestamp NOT NULL, data jsonb, processing_date date NOT NULL);"
    setup_cursor.execute(create_table_sql)
    truncate_table_sql = f"TRUNCATE TABLE {constants.POSTGRES_TABLE}"
    setup_cursor.execute(truncate_table_sql)
    setup_cursor.close()
    connection.close()


def main():
    pg_setup()
    validator = Validator()
    validator.process_file()


if __name__ == "__main__":
    main()
