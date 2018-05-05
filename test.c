#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include "jWrite.h"

void extract_error(
    char *fn,
    SQLHANDLE handle,
    SQLSMALLINT type)
{
    SQLINTEGER   i = 0;
    SQLINTEGER   native;
    SQLCHAR      state[ 7 ];
    SQLCHAR      text[256];
    SQLSMALLINT  len;
    SQLRETURN    ret;

    fprintf(stderr,
            "\n"
            "The driver reported the following diagnostics whilst running "
            "%s\n\n",
            fn);

    do
    {
        ret = SQLGetDiagRec(type, handle, ++i, state, &native, text,
                            sizeof(text), &len );
        if (SQL_SUCCEEDED(ret))
            printf("%s:%ld:%ld:%s\n", state, i, native, text);
    }
    while( ret == SQL_SUCCESS );
}


int main() {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret; /* ODBC API return status */
  SQLCHAR outstr[1024];
  SQLSMALLINT outstrlen;
  SQLSMALLINT columns; /* number of columns in result-set */
  int row = 0;
  unsigned int buflen= 1024;
  int err;
  char buffer[1024];
  char* dsn = {"Server=wolverine;Driver=/opt/mapr/drill/lib/64/libdrillodbc_sb64.so;\
  authenticationType=kerberos;connectionType=Direct;Port=31010;ZKClusterID=drillbits1;\
  KrbServiceHost=wolverine.6lines.com;KrbServiceName=drill"};

  /* Allocate an environment handle */
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  /* We want ODBC 3 support */
  SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
  /* Allocate a connection handle */
  SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  /* Connect to the DSN mydsn */
  /* You will need to change mydsn to one you have created and tested */
   ret = SQLDriverConnect(dbc, NULL, dsn, SQL_NTS,
                         outstr, sizeof(outstr), &outstrlen,
                         SQL_DRIVER_COMPLETE);
  if (SQL_SUCCEEDED(ret)) {
    printf("Connected\n");
    printf("Returned connection string was:\n\t%s\n", outstr);
    if (ret == SQL_SUCCESS_WITH_INFO) {
      printf("Driver reported the following diagnostics\n");
      extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    }

  } else {
    fprintf(stderr, "Failed to connect\n");
    extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    exit(-1);
  }
  //write some fake json
  jwOpen( buffer, buflen, JW_OBJECT, JW_PRETTY );
  jwObj_string( "key", "value" );				// add object key:value pairs
  jwObj_int( "int", 1 );
  jwObj_double( "double", 1.234 );
  err= jwClose();
  printf( buffer );
  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  /* Retrieve a list of tables */
  //SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, "TABLE", SQL_NTS);
  SQLExecDirect(stmt, "SELECT pid,exe FROM dfs.data.procs limit 2", SQL_NTS);
  /* How many columns are there */
  SQLNumResultCols(stmt, &columns);
  /* Loop through the rows in the result-set */
  while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      SQLUSMALLINT i;
      printf("Row %d\n", row++);
      /* Loop through the columns */
      for (i = 1; i <= columns; i++) {
          SQLINTEGER indicator;
          char buf[512];
          /* retrieve column data as a string */
          ret = SQLGetData(stmt, i, SQL_C_CHAR,
                           buf, sizeof(buf), &indicator);
          if (SQL_SUCCEEDED(ret)) {
              /* Handle null columns */
              if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
              printf("  Column %u : %s\n", i, buf);
          }
      }
  }
  SQLDisconnect(dbc); /* disconnect from driver */
}
