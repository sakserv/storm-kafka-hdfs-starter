package com.hortonworks.skumpf.minicluster.test;

import com.hortonworks.skumpf.minicluster.HiveLocalServer2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * Created by skumpf on 12/30/14.
 */
public class HiveLocalServer2Test {

    private static final String HIVE_DB_NAME = "testdb";
    private static final String HIVE_TABLE_NAME = "testtable";
    HiveLocalServer2 hiveServer;

    @Before
    public void setUp() {
        hiveServer = new HiveLocalServer2();
        hiveServer.start();
    }

    @After
    public void tearDown() {
        hiveServer.stop();
    }

    @Test
    public void testHiveLocalServer2() {

        // Load the Hive JDBC driver
        try {
            System.out.println("HIVE: Loading the Hive JDBC Driver");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        }

        // Create an ORC table and describe it
        try {

            Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveServer.getHiveServerThriftPort() + "/" + HIVE_DB_NAME, "user", "pass");

            String createDbDdl = "CREATE DATABASE IF NOT EXISTS " + HIVE_DB_NAME;
            Statement stmt = con.createStatement();
            System.out.println("HIVE: Running Create Database Statement: " + createDbDdl);
            stmt.execute(createDbDdl);


            String dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;

            stmt = con.createStatement();
            System.out.println("HIVE: Running Drop Table Statement: " + dropDdl);
            stmt.execute(dropDdl);

            String createDdl = "CREATE TABLE IF NOT EXISTS " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME + " (id INT, msg STRING) " +
                "PARTITIONED BY (dt STRING) " +
                "CLUSTERED BY (id) INTO 16 BUCKETS " +
                "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";

            stmt = con.createStatement();
            System.out.println("HIVE: Running Create Table Statement: " + createDdl);
            stmt.execute(createDdl);

            System.out.println("HIVE: Validating Table was Created: ");
            ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " + HIVE_TABLE_NAME);
            while (resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    System.out.print(resultSet.getString(i));
                }
                System.out.println();
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

    }

}
