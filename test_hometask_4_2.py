import os
import pyodbc
import pytest

# DATABASE PARAMETERS
SERVER = os.environ['db_host']
DATABASE = 'AdventureWorks2012'
USERNAME = os.environ['db_username']
PASSWORD = os.environ['db_password']


# Report configuration


@pytest.fixture
def setup_db_connection():
    """
    Connects to the AdventureWorks2012 database before performing the test
    cases and disconnects after the whole test suite is completed.
    """
    conn_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'
    conn = pyodbc.connect(conn_string)
    print(f'Connected to the {DATABASE} database.')
    yield conn
    conn.close()
    print(f'Disconnected from the {DATABASE} database.')


class TestDatabase:
    @pytest.mark.parametrize(
        "schema_name, table_name", [
            ('production', 'unitmeasure'),
            ('production', 'document'),
            ('person', 'address')
        ])
    def test_existence_of_the_table(
            self,
            setup_db_connection,
            schema_name,
            table_name
    ):
        """
        Check if the given schema name and table name exist in the database.

        Expected result: 1 row found.
        """
        print(TestDatabase.test_existence_of_the_table.__doc__)
        print(f'Parameters:\nschema_name = {schema_name}\ntable_name = {table_name}\n')

        expected_result = 1

        check_sql = f"""
        SELECT
            SCHEMA_NAME(tab.schema_id) AS schema_name,
            tab.name AS table_name
        FROM sys.tables AS tab
        WHERE
            LOWER(tab.name) = '{table_name}'
            AND LOWER(SCHEMA_NAME(tab.schema_id)) = '{schema_name}';"""

        cursor = setup_db_connection.cursor()
        cursor.execute(check_sql)
        records = cursor.fetchall()

        actual_result = len(records)
        print(f'Actual result: {actual_result} rows\nExpected result: {expected_result} rows')

        assert actual_result == expected_result

    @pytest.mark.depends(on=['test_existence_of_the_table'])
    @pytest.mark.parametrize(
        "schema_name, table_name, expected_result", [
            ('production', 'unitmeasure', ['modifieddate', 'name', 'unitmeasurecode']),
            ('production', 'document', ['changenumber', 'document', 'documentlevel', 'documentnode', 'documentsummary', 'fileextension', 'filename', 'folderflag', 'modifieddate', 'owner', 'revision', 'rowguid', 'status', 'title']),
            ('person', 'address', ['addressid', 'addressline1', 'addressline2', 'city', 'modifieddate', 'postalcode', 'rowguid', 'spatiallocation', 'stateprovinceid'])
        ])
    def test_table_structure(
            self,
            setup_db_connection,
            schema_name,
            table_name,
            expected_result
    ):
        """
        Check if the structure of the given schema name and table name
        matches expected list of column names.
        """
        print(TestDatabase.test_table_structure.__doc__)
        print(f'Parameters:\nschema_name = {schema_name}\ntable_name = {table_name}\n')

        check_sql = f"""
        SELECT LOWER(col.name) AS column_name
        FROM sys.tables AS tab
        LEFT JOIN sys.columns AS col ON col.object_id = tab.object_id 
        WHERE
            LOWER(tab.name) = '{table_name}'
            AND LOWER(SCHEMA_NAME(tab.schema_id)) = '{schema_name}'
        ORDER BY column_name;"""

        cursor = setup_db_connection.cursor()
        cursor.execute(check_sql)
        records = cursor.fetchall()

        actual_result = [row[0] for row in records]
        print(f'Actual result: {", ".join(actual_result)}\nExpected result: {",".join(expected_result)} rows')

        assert actual_result == expected_result

    @pytest.mark.depends(on=['test_existence_of_the_table', 'test_table_structure'])
    @pytest.mark.parametrize(
        "schema_name, table_name, expected_result", [
            ('production', 'unitmeasure', {'modifieddate': 'datetime', 'name': 'nvarchar', 'unitmeasurecode': 'nchar'}),
        ])
    def test_table_columns_data_types(
            self,
            setup_db_connection,
            schema_name,
            table_name,
            expected_result
    ):
        """
        Check if the datatypes of the given schema name and table name
        matches expected columns datatypes.
        """
        print(TestDatabase.test_table_columns_data_types.__doc__)
        print(f'Parameters:\nschema_name = {schema_name}\ntable_name = {table_name}\n')

        check_sql = f"""
        SELECT
            LOWER(col.name) AS column_name,
            LOWER(ty.name) AS column_type
        FROM sys.tables AS tab
        INNER JOIN sys.columns AS col ON col.object_id = tab.object_id
        INNER JOIN sys.types AS ty ON ty.system_type_id = col.system_type_id
        WHERE
            LOWER(SCHEMA_NAME(tab.schema_id)) = '{schema_name}'
            AND LOWER(SCHEMA_NAME(ty.schema_id)) = 'sys'
            AND (tab.name) = '{table_name}'
            AND ty.name != 'sysname'
        ORDER BY column_name;"""

        cursor = setup_db_connection.cursor()
        cursor.execute(check_sql)
        records = cursor.fetchall()

        actual_result = {row[0]: row[1] for row in records}
        print(f'Actual result:', actual_result, '\nExpected result:\n', expected_result)

        assert actual_result == expected_result

    @pytest.mark.depends(on=['test_existence_of_the_table', 'test_table_structure', 'test_table_columns_data_types'])
    @pytest.mark.parametrize(
        "schema_name, table_name, column_name, expected_values", [
            ('production', 'document', 'folderflag', '0, 1'),
            ('production', 'document', 'status', '1, 2, 3'),
        ])
    def test_table_column_for_correct_values(
            self,
            setup_db_connection,
            schema_name,
            table_name,
            column_name,
            expected_values
    ):
        """
        Check if the values given schema name, table name and column name
        matches expected values.

        Expected result: 0 rows found
        """
        print(TestDatabase.test_table_column_for_correct_values.__doc__)
        print(f'Parameters:\nschema_name = {schema_name}\ntable_name = {table_name}\ncolumn_name = {column_name}\n')

        expected_result = 0

        check_sql = f"""
        SELECT DISTINCT {column_name}
        FROM {schema_name}.{table_name}
        WHERE {column_name} NOT IN ({expected_values});"""

        cursor = setup_db_connection.cursor()
        cursor.execute(check_sql)
        records = cursor.fetchall()

        actual_result = len(records)
        print(f'Actual result: {actual_result} rows\nExpected result: {expected_result} rows')

        assert actual_result == expected_result

    @pytest.mark.depends(on=['test_existence_of_the_table', 'test_table_structure', 'test_table_columns_data_types'])
    def test_production_document_documentlevel_validity(self, setup_db_connection):
        """
        Test if the DocumentLevel column values are no less than 0.

        Expected result: 0 rows found.
        """
        print(TestDatabase.test_production_document_documentlevel_validity.__doc__)
        expected_result = 0

        check_sql = f"""
        SELECT DISTINCT DocumentLevel
        FROM Production.Document
        WHERE DocumentLevel < 0;"""

        cursor = setup_db_connection.cursor()
        cursor.execute(check_sql)
        records = cursor.fetchall()

        actual_result = len(records)
        print(f'Actual result: {actual_result} rows\nExpected result: {expected_result} rows')

        assert actual_result == expected_result

    @pytest.mark.depends(on=['test_existence_of_the_table', 'test_table_structure', 'test_table_columns_data_types'])
    def test_production_unitmeasure_unitmeasurecode_consistency(
            self,
            setup_db_connection
    ):
        """
        Test if the values in the Purchasing.ProductVendor.UnitMeasureCode column
        exists in the Production.UnitMeasure.UnitMeasureCode.

        Expected result: 0 rows found
        """
        print(TestDatabase.test_production_unitmeasure_unitmeasurecode_consistency.__doc__)

        expected_result = 0

        check_sql = f"""
        SELECT
            u.UnitMeasureCode AS production_unit,
            p.UnitMeasureCode AS purchasing_unit
        FROM Production.UnitMeasure AS u
        FULL JOIN Purchasing.ProductVendor AS p ON p.UnitMeasureCode = u.UnitMeasureCode
        WHERE u.UnitMeasureCode IS NULL;"""

        cursor = setup_db_connection.cursor()
        cursor.execute(check_sql)
        records = cursor.fetchall()

        actual_result = len(records)
        print(f'Actual result: {actual_result} rows\nExpected result: {expected_result} rows')

        assert actual_result == expected_result
