package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * NOT rename this java file as Sort. it would run default Sort file instead of
 * this one
 * 
 * **/

public class WordSortAndDedup {

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable data = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable());
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable lineNum = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            // for(IntWritable value:values){
            context.write(lineNum, key);
            lineNum = new IntWritable(lineNum.get() + 1);
            // }
        }
    }

    public static class Partition extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int Maxnumber = 65523;
            int bound = Maxnumber / numPartitions + 1;
            int keynumber = key.get();
            for (int i = 0; i < numPartitions; i++) {
                if (keynumber < bound * (i + 1) && keynumber >= bound * i) {
                    return i;
                }
            }

            return -1;
        }

    }

    public static void main(String[] args) throws Exception {

        Log LOG = LogFactory.getLog(WordSortAndDedup.class);
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("err_Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count with sort");

        job.setJarByClass(WordSortAndDedup.class);
        job.setMapperClass(Map.class);
        // job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(Partition.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
