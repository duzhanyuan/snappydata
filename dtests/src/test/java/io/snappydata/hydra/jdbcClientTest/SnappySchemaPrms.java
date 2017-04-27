package io.snappydata.hydra.jdbcClientTest;

import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappySchemaPrms extends SnappyPrms {

  public static Long tablesList;

  public static Long snappyDDLExtn;

  public static Long dataFileLocation;

  public static Long csvFileNames;

  public static Long selectStmts;

  public static String[] getTableNames() {
    Long key = tablesList;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }

  public static String[] getSnappyDDLExtn() {
    Long key = snappyDDLExtn;
    Vector ddlExtn = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[ddlExtn.size()];
    for (int i = 0; i < ddlExtn.size(); i++) {
      strArr[i] = (String)ddlExtn.elementAt(i);
    }
    return strArr;
  }

  public static String[] getCSVFileNames() {
    Long key = csvFileNames;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i);
    }
    return strArr;
  }

  public static String getDataLocations() {
    Long key = dataFileLocation;
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, null));
  }

  public static String[] getSelectStmts(){
    Long key = selectStmts;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  static {
    SnappyPrms.setValues(SnappySchemaPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
