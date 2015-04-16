# -*- coding: utf-8 -*-

ENCODING = 'utf-8'

ORACLE_HOST = None
ORACLE_PORT = 1521
ORACLE_SID  = "orcl"
ORACLE_USER = "pyramid_test"
ORACLE_PW   = "2324024"


POSTGRES_HOST = 'localhost'
POSTGRES_PORT = 5432
POSTGRES_DBNAME = 'pyramid_test'
POSTGRES_USER = 'pyramid_test'
POSTGRES_PW = '123'

POSTGRES_DSN = "host='%s' port=%s dbname='%s' user='%s' password='%s'" % \
    (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER, POSTGRES_PW)

MSSQL_HOST = None
MSSQL_DBNAME = 'mssql_dbname'
MSSQL_USER = 'mssql_user'
MSSQL_PW = 'mssql_password'


# PostgreSQL DSN
PG_ODBC_DSN = "DRIVER={PostgreSQL Unicode};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s" % \
    (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER, POSTGRES_PW)

# Oracle DSN
ORA_ODBC_DSN = "DRIVER={Oracle in OraClient10g_x86};DBQ=%s:%s/%s;UID=%s;PWD=%s" % \
    (ORACLE_HOST, ORACLE_PORT, ORACLE_SID, ORACLE_USER, ORACLE_PW)

# Microsoft SQL Server DSN
MS_ODBC_DSN = None


# Name of default ormlite's DB connection module (psycopgcon, cxoracon, odbcmscon)
DEFAULT_DB_CON = 'psycopgcon'
#DEFAULT_DB_CON = 'cxoracon'
print '[Default DB: %s] ' % DEFAULT_DB_CON

# Type of default DB
DEFAULT_DB_TYPE = 'pg'  # Valid types: 'pg', 'ora', 'ms'
#DEFAULT_DB_TYPE = 'ora'  # Valid types: 'pg', 'ora', 'ms'
