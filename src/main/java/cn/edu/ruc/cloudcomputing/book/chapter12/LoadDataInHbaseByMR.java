package cn.edu.ruc.cloudcomputing.book.chapter12;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/* MapReduce 读取hdfs上的文件，以HTable.put(put)的方式在map中完成数据写入，无reduce过程*/
public class LoadDataInHbaseByMR extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(LoadDataInHbaseByMR.class);

    public static final String JOBNAME = "LoadDataInHbaseByMapReduce";

    public static class Map extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

        Configuration configuration = null;

        HTable aHTable = null;

        private boolean wal = true;

        static long count = 0;

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            aHTable.flushCommits();
            aHTable.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            Put put = null;
            String all[] = value.toString().split("/t");

            if (all.length == 2) {
                put = new Put(Bytes.toBytes(all[0]));
                put.add(Bytes.toBytes("xxx"), Bytes.toBytes("20110313"), Bytes.toBytes(all[1]));
            }

            if (!wal) {
                put.setWriteToWAL(false);
            }

            aHTable.put(put);

            if ((++count % 100) == 0) {
                context.setStatus(count + " DOCUMENTS done!");
                context.progress();
                System.out.println(count + " DOCUMENTS done!");
            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            configuration = context.getConfiguration();
            aHTable = new HTable(configuration, "testKang");
            aHTable.setAutoFlush(false);
            aHTable.setWriteBufferSize(12 * 1024 * 1024);
            wal = true;

        }

    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];

        Configuration conf = HBaseConfiguration.create(getConf());

        conf.set("hbase.master", "m0:60000");

        Job job = new Job(conf, JOBNAME);

        job.setJarByClass(LoadDataInHbaseByMR.class);

        job.setMapperClass(Map.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(NullOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int res = 1;
        
        try {
            res = ToolRunner.run(conf, new LoadDataInHbaseByMR(), otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(res);

    }

}