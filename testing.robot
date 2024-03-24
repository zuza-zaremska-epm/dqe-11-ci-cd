*** Settings ***
Library  DatabaseLibrary
Library    OperatingSystem
#Suite Setup    Connect To Database    pymssql    ${dbName}    ${db_username}    ${db_password}    ${db_host}    ${db_port}
#Suite Teardown    Disconnect From Database

*** Variables ***
${dbName}    AdventureWorks2012
${db_username}    TestLogin
${db_password}    pass123
${db_host}    127.0.0.1
${db_port}    1433

*** Test Cases ***
Control passing test
    Should Be Equal As Integers    1    1
