package com.hortonworks.skumpf.minicluster;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;

import java.security.Permission;

/**
 * Created by skumpf on 12/29/14.
 */
public class HiveLocalMetaStore {

    private static final String msPort = "20102";
    private static HiveConf hiveConf;
    private static SecurityManager securityManager;
    private Thread t;

    public static class NoExitSecurityManager extends SecurityManager {

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
            throw new RuntimeException("System.exit() was called. Raising exception. ");
        }
    }

    private static class RunMS implements Runnable {

        @Override
        public void run() {
            try {
                HiveMetaStore.startMetaStore(Integer.parseInt(msPort), new HadoopThriftAuthBridge(), HiveLocalMetaStore.configure());
//                HiveMetaStore.main(new String[]{"-v", "-p", msPort});
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public static HiveConf configure() {
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
        hiveConf = new HiveConf();
        hiveConf.set("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        hiveConf.set("hive.compactor.initiator.on", "true");
        hiveConf.set("hive.compactor.worker.threads", "5");
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true");
        //hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("hive.metastore.warehouse.dir", "/tmp/warehouse_dir");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.set("hive.root.logger", "DEBUG,console");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
                + msPort);
        //hiveConf.set("datanucleus.autoCreateSchema", "true");
        //hiveConf.set("datanucleus.autoCreateTables", "true");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
                "false");
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
        return hiveConf;
    }

    public void stop() throws Exception {
        System.setSecurityManager(securityManager);
        TxnDbUtil.setConfValues(configure());
        t.interrupt();
        //TxnDbUtil.cleanDb();
    }

    public void start() throws Exception {

        t = new Thread(new RunMS());
        t.start();
        Thread.sleep(5000);

        System.out.println("HIVE METASTORE: Prepping the database");
        TxnDbUtil.setConfValues(configure());
        TxnDbUtil.prepDb();

    }

    public String getMetaStoreThriftPort() {
        return msPort;
    }

    public void dumpMetaStoreConf() {
        System.out.println("HIVE METASTORE CONF: " + String.valueOf(hiveConf.getAllProperties()));
    }

    public String getMetaStoreUri() {
        return hiveConf.get("hive.metastore.uris");
    }

    public HiveConf getConf() {
        return hiveConf;
    }

}
