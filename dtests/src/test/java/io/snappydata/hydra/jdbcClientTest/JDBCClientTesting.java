package io.snappydata.hydra.jdbcClientTest;


import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.HydraVector;
import hydra.Log;
import hydra.Prms;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyTest;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class JDBCClientTesting extends SnappyTest {

  protected static JDBCClientTesting testInstance;

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new JDBCClientTesting();
  }

  public static void HydraTask_createSnappySchemas() {
    testInstance.createSnappySchemas();
  }

  protected void createSnappySchemas() {
    try{
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("creating schemas in snappy.");
      createSchemas(conn);
      Log.getLogWriter().info("done creating schemas in snappy.");
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void createSchemas(Connection conn) {
    String[] schemas = SQLPrms.getSchemas();
    StringBuffer aStr = new StringBuffer("Created schemas \n");
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < schemas.length; i++) {
        s.execute(schemas[i]);
        Object o = schemas[i];
        aStr.append(o.toString() + "\n");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Y68")) {
        Log.getLogWriter().info("got schema existing exception if multiple threads" +
            " try to create schema, continuing tests");
      } else
        SQLHelper.handleSQLException(se);
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public static synchronized void HydraTask_createSnappyTables(){
    testInstance.createSnappyTables();
  }

  protected void createSnappyTables() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("dropping tables in snappy.");
      dropTables(conn); //drop table before creating it
      Log.getLogWriter().info("done dropping tables in snappy");
      Log.getLogWriter().info("creating tables in snappy.");
      createTables(conn);
      Log.getLogWriter().info("done creating tables in snappy, now loading the data.");
      runGemXDQuery = true;
      conn = getClientConnection();
      loadTables(conn);
      runGemXDQuery = false;
      closeConnection(conn);

    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void createTables(Connection conn) {
    //to get create table statements from config file
    String[] createTablesDDL = getCreateTablesStatements(true);
    String[] ddlExtn = SnappySchemaPrms.getSnappyDDLExtn();
    StringBuffer aStr = new StringBuffer("Created tables \n");
    try {
      Statement s = conn.createStatement();
        for (int i = 0; i < createTablesDDL.length; i++) {
          String createDDL = createTablesDDL[i] + ddlExtn[i];
          Log.getLogWriter().info("about to create table : " + createDDL);
          s.execute(createDDL);
          Log.getLogWriter().info("Created table " + createDDL);
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create tables\n"
          + TestHelper.getStackTrace(se));
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public void loadTables(Connection conn){
    String[] tableNames = SnappySchemaPrms.getTableNames();
    String[] csvFileNames = SnappySchemaPrms.getCSVFileNames();
    String dataLocation = SnappySchemaPrms.getDataLocations();
    for (int i = 0; i < tableNames.length; i++) {
      String tableName = tableNames[i].toUpperCase();
      Log.getLogWriter().info("Loading data into "+ tableName);
      String[] table = tableName.split("\\.");
      String csvFilePath = dataLocation + File.separator + csvFileNames[i];
      Log.getLogWriter().info("CSV location is : " + csvFilePath);
      try {
        PreparedStatement ps = conn.prepareStatement(
            "CALL SYSCS_UTIL.IMPORT_TABLE_EX(?,?,?,null,null,null,0,0,6,0,null,null)");
        ps.setString(1, table[0]);
        ps.setString(2, table[1]);
        ps.setString(3, csvFilePath);
        //ps.setString(4, ",");
        ps.execute();
        Log.getLogWriter().info("Loaded data into " + tableNames[i]);
      } catch (SQLException se) {
        throw new TestException("Exception while loading data into table.Exception is " + se
            .getSQLState() + " : " + se.getMessage());
      }
    }
  }

  protected void dropTables(Connection conn) {
    String sql = null;
    String[] tables = SnappySchemaPrms.getTableNames();
    sql = "drop table if exists ";
    try {
      for (String table : tables) {
        Statement s = conn.createStatement();
        s.execute(sql + table);
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while dropping table.", se);
    }
  }

  public static String[] getCreateTablesStatements(boolean forDerby) {
    Long key = SQLPrms.createTablesStatements;
    Vector statements = TestConfig.tab().vecAt(key, new HydraVector());
    String[] strArr = new String[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      strArr[i] = (String)statements.elementAt(i);
    }
    return strArr;
  }

  /*
Hydra task to execute select queries
*/
  public static void HydraTask_executeQueries() {
    testInstance.executeQuery();
  }

  public void executeQuery() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      String selectStmt[] = SnappySchemaPrms.getSelectStmts();
      ResultSet snappyRS;
      int rand = new Random().nextInt(selectStmt.length);
      String query = selectStmt[rand];
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      try {
        snappyRS = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
      } catch(SQLException se){
        if(se.getSQLState().equals("21000") || se.getSQLState().equals("0A000") ){
          //retry select query with routing
          Log.getLogWriter().info("Got exception while executing select query, retrying with " +
              "executionEngine as spark.");
          String query1 = query +  " --GEMFIREXD-PROPERTIES executionEngine=Spark";
          snappyRS = conn.createStatement().executeQuery(query1);
          Log.getLogWriter().info("Executed query on snappy.");
        }else throw new SQLException(se);
      }
      int numRows =0 ;
      while(snappyRS.next()) numRows++;
      Log.getLogWriter().info("Num rows in resultSet is:" + numRows);
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);
      snappyRS.close();
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  /* Verify results at the end of the test*/
  public static void HydraTask_verifyResults() {
    testInstance.verifyResults();
  }

  public void verifyResults() {

    StringBuffer mismatchString = new StringBuffer();
    String tableName="";
    try {
      String[] tables = SnappySchemaPrms.getTableNames();
      String stmt = "select * from ";
      Connection conn = getLocatorConnection();
      for (String table : tables) {
        tableName = table;
        String selectStmt = stmt + table;
        Log.getLogWriter().info("Verifying results for " + table + " using " + selectStmt);
        ResultSet snappyRS = conn.createStatement().executeQuery(stmt + table);
        int numRows = 0 ;
        while(snappyRS.next()) numRows++;
        Log.getLogWriter().info("Num rows in resultSet is:" + numRows);

      }
    }catch(SQLException se){
      throw new TestException("Got Exception while verifying the table data.",se);
    }
  }

}
