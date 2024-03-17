*** Settings ***
Library  DatabaseLibrary
Library    OperatingSystem
Suite Setup    Connect To Database    pymssql    ${dbName}    ${db_username}    ${db_password}    ${db_host}    ${db_port}
Suite Teardown    Disconnect From Database

*** Variables ***
${dbName}    AdventureWorks2012
${db_username}    Test_Login
${db_password}    pass123
${db_host}    127.0.0.1
${db_port}    1433

*** Test Cases ***
Check duplicates in Person.Address
    [Tags]    Person.Address
    [Documentation]
    ...    | *Setup:*
    ...    | AdventureWorks2012 database is up and running.
    ...    | Connection to database was established successfully.
    ...    |
    ...    | *Test steps:*
    ...    | 1. Select records from Person.Address table having the same data
    ...    | within the following set of columns:
    ...    | AddressLine1, AddressLine2, City, Postal Code.
    ...    |
    ...    | *Expected result:*
    ...    | 0 rows found.
    ${query_result}    Query    SELECT COUNT(*) AS ctn, UPPER(addressline1), UPPER(addressline2), UPPER(city), postalcode FROM Person.Address GROUP BY addressline1, addressline2, city, postalcode HAVING COUNT(*) > 1;
    ${query_len}    Get Length    ${query_result}
    Run Keyword If    ${query_len} != 0    Log    ${query_result}
    Should Be Equal As Integers    ${query_len}    0

Check Person.Address for NULL values
    [Tags]    Person.Address
    [Documentation]
    ...    | *Setup:*
    ...    | AdventureWorks2012 database is up and running.
    ...    | Connection to database was established successfully.
    ...    |
    ...    | *Test steps:*
    ...    | 1. Select records from Person.Address table having NULL values
    ...    | in the following not nullable columns:
    ...    | AddressID, AddressLine1, City, StateProvinceID, PostalCode, rowguid, ModifiedDate
    ...    |
    ...    | *Expected result:*
    ...    | 0 rows found.
    ${queryresult}    Query    SELECT * FROM Person.Address WHERE AddressID IS NULL OR AddressLine1 IS NULL OR City IS NULL OR StateProvinceID IS NULL OR PostalCode IS NULL OR rowguid IS NULL OR ModifiedDate IS NULL;
    ${query_len}    Get Length    ${query_result}
    Run Keyword If    ${query_len} != 0    Log    ${query_result}
    Should Be Equal As Integers    ${query_len}    0

Check Production.UnitMeasure.UnitMeasureCode primary key
    [Tags]    Production.UnitMeasure
    [Documentation]
    ...    | *Setup:*
    ...    | AdventureWorks2012 database is up and running.
    ...    | Connection to database was established successfully.
    ...    |
    ...    | *Test steps:*
    ...    | 1. Select records from Person.UnitMeasure table having duplicated
    ...    | or NULL values in primary key UnitMeasureCode column.
    ...    |
    ...    | *Expected result:*
    ...    | 0 rows found.
    ${query_result}    Query    SELECT * FROM (SELECT UnitMeasureCode, COUNT(*) OVER(PARTITION BY UnitMeasureCode) AS occurrences, CASE WHEN UnitMeasureCode IS NULL THEN 'Y' ELSE 'N' END AS check_if_null FROM Production.UnitMeasure) AS sub WHERE occurrences > 1 OR check_if_null = 'Y';
    ${query_len}    Get Length    ${query_result}
    Run Keyword If    ${query_len} != 0    Log    ${query_result}
    Should Be Equal As Integers    ${query_len}    0

Check Production.UnitMeasure data length
    [Tags]    Production.UnitMeasure
    [Documentation]
    ...    | *Setup:*
    ...    | AdventureWorks2012 database is up and running.
    ...    | Connection to database was established successfully.
    ...    |
    ...    | *Test steps:*
    ...    | 1. Select records from Person.UnitMeasure table having data not
    ...    | equal 3 characters in UnitMeasureCode column and data longer than
    ...    | 50 characters in Name column.
    ...    |
    ...    | *Expected result:*
    ...    | 0 rows found.
    ${query_result}    Query    SELECT UnitMeasureCode, Name, CASE WHEN LEN(UnitMeasureCode) != 3 AND LEN(Name) > 50 THEN 'unit, name' WHEN LEN(UnitMeasureCode) != 3 THEN 'unit' WHEN LEN(Name) > 50 THEN 'name' ELSE NULL END AS flag FROM Production.UnitMeasure WHERE LEN(UnitMeasureCode) != 3 OR LEN(name) > 50;
    ${query_len}    Get Length    ${query_result}
    Run Keyword If    ${query_len} != 0    Log    ${query_result}
    Should Be Equal As Integers    ${query_len}    0
    
Check Production.Document.Status values
    [Tags]    Production.Document
    [Documentation]
    ...    | *Setup:*
    ...    | AdventureWorks2012 database is up and running.
    ...    | Connection to database was established successfully.
    ...    |
    ...    | *Test steps:*
    ...    | 1. Select records from Production.Document table having values in
    ...    | Status column not equal 1, 2 or 3.
    ...    |
    ...    | *Expected result:*
    ...    | 0 rows found.
    ${query_result}    Query    SELECT Status FROM Production.Document WHERE Status NOT IN (1, 2, 3);
    ${query_len}    Get Length    ${query_result}
    Run Keyword If    ${query_len} != 0    Log    ${query_result}
    Should Be Equal As Integers    ${query_len}    0

Check Production.Document.Owner foreign key consistency
    [Tags]    Production.Document
    [Documentation]
    ...    | *Setup:*
    ...    | AdventureWorks2012 database is up and running.
    ...    | Connection to database was established successfully.
    ...    |
    ...    | *Test steps:*
    ...    | 1. Select records from Production.Document table having values in
    ...    | Owner column that don't exist in the BusinessEntityID column
    ...    | in the HumanResources.Employee table.
    ...    |
    ...    | *Expected result:*
    ...    | 0 rows found.
    ${query_result}    Query    SELECT Owner FROM Production.Document WHERE Owner NOT IN (SELECT DISTINCT BusinessEntityID FROM HumanResources.Employee);
    ${query_len}    Get Length    ${query_result}
    Run Keyword If    ${query_len} != 0    Log    ${query_result}
    Should Be Equal As Integers    ${query_len}    0
