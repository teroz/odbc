#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include "jWrite.h"

#include "util.c"

#define BUFFERSIZE      1024
#define NUMCOLS         5

struct DataBinding {
   SQLSMALLINT TargetType;
   SQLPOINTER TargetValuePtr;
   SQLINTEGER BufferLength;
   SQLLEN StrLen_or_Ind;
};

SQLRETURN printStatementResult(SQLHSTMT hstmt) {

    int i;
    SQLRETURN retcode = SQL_SUCCESS;
    SQLSMALLINT numColumns = 0, bufferLenUsed;
    SQLPOINTER* columnLabels = NULL;

    struct DataBinding* columnData = NULL;

    retcode = SQLNumResultCols(hstmt, &numColumns);
    CHECK_ERROR(retcode, "SQLNumResultCols()",
                hstmt, SQL_HANDLE_STMT);

    printf ("DataBinding Size : %i\n",
                (int)(numColumns * sizeof(struct DataBinding)));
    columnData = (struct DataBinding*)
                malloc ( numColumns * sizeof(struct DataBinding) );
    columnLabels = (SQLPOINTER *)malloc( numColumns * sizeof(SQLPOINTER*) );

    for ( i = 0 ; i < numColumns ; i++ ) {
        columnData[i].TargetValuePtr = NULL;
        columnLabels[i] = NULL;
    }

    printf( "No of columns : %i\n", numColumns );
    for ( i = 0 ; i < numColumns ; i++ ) {
        columnLabels[i] = (SQLPOINTER)malloc( BUFFERSIZE*sizeof(char) );

        // Get Field names from Table
        retcode = SQLColAttribute(hstmt, (SQLUSMALLINT)i + 1, SQL_DESC_NAME,
                            columnLabels[i], (SQLSMALLINT)BUFFERSIZE,
                            &bufferLenUsed, NULL);
        CHECK_ERROR(retcode, "SQLColAttribute()",
                    hstmt, SQL_HANDLE_STMT);

        printf( "Column %d: %s\n", i+1, (SQLCHAR*)columnLabels[i] );
    }

   // Allocate memory for the binding
    for ( i = 0 ; i < numColumns ; i++ ) {
        columnData[i].TargetType = SQL_C_CHAR;
        columnData[i].BufferLength = (BUFFERSIZE+1);
        columnData[i].TargetValuePtr =
                malloc( sizeof(unsigned char)*columnData[i].BufferLength );
    }

    // Set up the binding
    for ( i = 0 ; i < numColumns ; i++ ) {
        printf ("Binding Column %i\n", i+1);
        printf (" TargetType : %i\n", columnData[i].TargetType);
        printf (" ValuePtr : %p\n", columnData[i].TargetValuePtr);
        printf (" ColumnDataLen %i\n", columnData[i].BufferLength);
        printf (" StrLen_or_Ind %p\n", &(columnData[i].StrLen_or_Ind));
        retcode = SQLBindCol(hstmt, (SQLUSMALLINT)i+1,
                    columnData[i].TargetType, columnData[i].TargetValuePtr,
                    columnData[i].BufferLength, &(columnData[i].StrLen_or_Ind));
        CHECK_ERROR(retcode, "SQLBindCol(1)",
                    hstmt, SQL_HANDLE_STMT);
    }

    printf( "Data :\n" );
    // Fetch the data and print out the data
    for ( retcode = SQLFetch(hstmt) ;
          retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO ;
          retcode = SQLFetch(hstmt) ) {
        int j;
        printf ("\n");
        for ( j = 0 ; j < numColumns ; j++ ) {
            printf( "%s: %.12s\n", (char *) columnLabels[j],
                                (char *) columnData[j].TargetValuePtr );
            memset (columnData[j].TargetValuePtr, ' ', BUFFERSIZE+1);
        }
    }

    // If we've just read all the data return success
    if (retcode==SQL_NO_DATA) retcode=SQL_SUCCESS;

    printf( "\n" );

exit:

    // Free buffers
    for ( i = 0 ; i < numColumns ; i++ ) {
        if (columnLabels[i] != NULL) free (columnLabels[i]);
    }
    for ( i = 0 ; i < numColumns ; i++ ) {
        if (columnData[i].TargetValuePtr != NULL)
            free (columnData[i].TargetValuePtr);
    }

    if (columnLabels!=NULL) free (columnLabels);
    if (columnData!=NULL) free (columnData);

    return retcode;
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
  /* Allocate a statement handle */
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  /* Retrieve a list of tables */
  //SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, "TABLE", SQL_NTS);
  ret = SQLExecDirect(stmt, "SELECT pid,exe FROM dfs.data.procs limit 2", SQL_NTS);
  CHECK_ERROR(ret, "SQLExecDirect()", stmt, SQL_HANDLE_STMT);

  ret = printStatementResult(stmt);
  CHECK_ERROR(ret, "printStatementResult()",
                    stmt, SQL_HANDLE_STMT);
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
exit:

    printf ("\nComplete.\n");

    // Free handles
    // Statement
    if (stmt != SQL_NULL_HSTMT)
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    // Connection
    if (dbc != SQL_NULL_HDBC) {
        SQLDisconnect(dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }

    // Environment
    if (env != SQL_NULL_HENV)
        SQLFreeHandle(SQL_HANDLE_ENV, env);

    return 0;
}
