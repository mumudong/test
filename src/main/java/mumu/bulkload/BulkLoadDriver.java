package mumu.bulkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class BulkLoadDriver extends Configured implements Tool {
    private static final String DATA_SEPERATOR = "\\s+";
    private static final String TABLE_NAME = "test";
    private static final String COLUMN_FAMILY_1="col1";
    private static final String COLUMN_FAMILY_2="col2";

    public static void main(String[] args) {
        try {
            int response = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadDriver(), args);
            if(response == 0) {
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        String outputPath = args[1];
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = getConf();
//        configuration.set("fs.default.name", "hdfs://mu1:8020");
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("mapred.jar", "E:\\test\\target\\test-1.0-SNAPSHOT-jar-with-dependencies.jar");
        configuration.set("data.seperator", DATA_SEPERATOR);
        configuration.set("hbase.table.name", TABLE_NAME);
        configuration.set("COLUMN_FAMILY_1", COLUMN_FAMILY_1);
        configuration.set("COLUMN_FAMILY_2", COLUMN_FAMILY_2);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "mu1,mu2,mu3");
        configuration.set("fs.defaultFS","hdfs://cluster");
        configuration.set("dfs.nameservices", "cluster");
        configuration.set("dfs.ha.namenodes.cluster", "nn1,nn2");
        configuration.set("dfs.namenode.rpc-address.cluster.nn1", "mu1:8020");
        configuration.set("dfs.namenode.rpc-address.cluster.nn2", "mu1:8020");
        configuration.set("dfs.client.failover.proxy.provider.cluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        configuration.set("yarn.resourcemanager.hostname","mu4");

        Job job = Job.getInstance(configuration, "Bulk Loading HBase Table::" + TABLE_NAME);
        job.setJarByClass(BulkLoadDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setMapperClass(BulkLoadMapper.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path(outputPath);
        if (fs.exists(output)) {
            fs.delete(output, true);//
        }
        FileOutputFormat.setOutputPath(job, output);//
        Connection connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf(TABLE_NAME);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
        job.waitForCompletion(true);
        if (job.isSuccessful()){
            System.out.println("success...");
//            HFileLoader.doBulkLoad(outputPath, TABLE_NAME);
            return 0;
        } else {
            System.out.println("fail...");
            return 1;
        }
    }

}