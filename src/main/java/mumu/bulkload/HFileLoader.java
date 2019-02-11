package mumu.bulkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 *
 */
public class HFileLoader {
    public static void main(String[] args) {
        doBulkLoad(args[0],"test");
    }
    public static void doBulkLoad(String pathToHFile, String tableName){
        try {
            Configuration configuration = new Configuration();
            configuration.set("mapreduce.app-submission.cross-platform", "true");
            configuration.set("mapreduce.framework.name", "yarn");
            configuration.set("mapred.jar", "E:\\test\\target\\test-1.0-SNAPSHOT-jar-with-dependencies.jar");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "mu1,mu2,mu3");
            configuration.set("fs.defaultFS","hdfs://cluster");
            configuration.set("dfs.nameservices", "cluster");
            configuration.set("dfs.ha.namenodes.cluster", "nn1,nn2");
            configuration.set("dfs.namenode.rpc-address.cluster.nn1", "mu1:8020");
            configuration.set("dfs.namenode.rpc-address.cluster.nn2", "mu1:8020");
            configuration.set("dfs.client.failover.proxy.provider.cluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            configuration.set("yarn.resourcemanager.hostname","mu4");
            HBaseConfiguration.addHbaseResources(configuration);
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
            HTable hTable = new HTable(configuration, tableName);
            loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);
            System.out.println("Bulk Load Completed..");
        } catch(Exception exception) {
            exception.printStackTrace();
        }

    }

}