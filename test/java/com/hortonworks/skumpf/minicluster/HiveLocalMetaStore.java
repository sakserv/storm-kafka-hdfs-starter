package com.hortonworks.skumpf.minicluster;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;

import java.security.Permission;

/**
 * Created by skumpf on 12/29/14.
 */
public class HiveLocalMetaStore implements MiniCluster {

    private static int msPort;
    private static HiveConf hiveConf = new HiveConf();
    private static SecurityManager securityManager;
    private Thread t;

    public class NoExitSecurityManager extends SecurityManager {

        @Override
        public void checkPermission(Permission perm) {
            // allow anything.
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }

        @Override
        public void checkExit(int status) {

            super.checkExit(status);
        }
    }

    private static class StartHiveLocalMetaStore implements Runnable {

        @Override
        public void run() {
            try {
                HiveMetaStore.startMetaStore(msPort, new HadoopThriftAuthBridge(), HiveLocalMetaStore.getConf());
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public HiveLocalMetaStore() {
        this(20102);
    }

    public HiveLocalMetaStore(int msPort) {
        this.msPort = msPort;
        configure(msPort);
    }

    public void configure() {
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
        hiveConf.set("hive.root.logger", "DEBUG,console");
        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=metastore_db;create=true");
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "/tmp/warehouse_dir");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
                "false");
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
    }

    public void configure(int msPort) {
        configure();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
                + msPort);
    }

    public void stop() {
        cleanDb();
        t.interrupt();
    }

    public void start() {
        t = new Thread(new StartHiveLocalMetaStore());
        t.start();
        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        prepDb();
    }

    public void prepDb() {
        try {
            System.out.println("HIVE METASTORE: Prepping the database");
            TxnDbUtil.setConfValues(getConf());
            TxnDbUtil.prepDb();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanDb() {
        try {
            System.out.println("HIVE METASTORE: Cleaning up the database");
            TxnDbUtil.setConfValues(getConf());
            TxnDbUtil.cleanDb();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public String getMetaStoreUri() {
        return hiveConf.get("hive.metastore.uris");
    }

    public static HiveConf getConf() {
        return hiveConf;
    }

    public void dumpConfig() {
        System.out.println("HIVE METASTORE CONFIG: " + String.valueOf(hiveConf.getAllProperties()));
    }

}
