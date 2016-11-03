package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;

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

public class Chapter05E3WordSortAndPositionDebup {

    private static IntWritable bigGlobalNum = new IntWritable(1);

    public static class Chapter05E3WordSortAndPositionMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable globalNum = new IntWritable(1);

        private IntWritable data = new IntWritable();

        private Context context;

        public void setup(Context context) {
            this.context = context;
            System.out.println("setup count=" + this.context.getConfiguration().get("count"));
        }

        public void map(Object key, Text value, Context context_out) throws IOException, InterruptedException {

            String count_value = this.context.getConfiguration().get("count");
            System.out.println("map count_value=" + this.context.getConfiguration().get("count"));

            IntWritable data_conf = new IntWritable(Integer.parseInt(count_value));
            IntWritable localNum = new IntWritable(1);

            System.out.println("Orignal value: localNum = " + localNum.get() + " globalNum = " + globalNum.get()
                    + " bigGlobalNum = " + bigGlobalNum.get() + " data_conf = " + data_conf.get());

            localNum = new IntWritable(localNum.get() + 11);

            globalNum = new IntWritable(globalNum.get() + 100);

            bigGlobalNum = new IntWritable(bigGlobalNum.get() + 10);

            data_conf = new IntWritable(data_conf.get() + 15);

            this.context.getConfiguration().set("count", data_conf.toString());

            String line = value.toString();

            data.set(Integer.parseInt(line));
            context_out.write(data, new IntWritable(1));
            System.out.println("line content: " + line);
            System.out.println("new value: localNum = " + localNum.get() + " globalNum = " + globalNum.get()
                    + " bigGlobalNum = " + bigGlobalNum.get() + " data_conf = " + data_conf.get());
        }
    }

    public static class Chapter05E3WordSortAndPositionReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable lineNum = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            // 去掉重复的内容
            // for (IntWritable value : values) {
            context.write(lineNum, key);
            lineNum = new IntWritable(lineNum.get() + 1);
            // }
        }
    }

    public static class Chapter05E3WordSortAndPositionPartition extends Partitioner<IntWritable, IntWritable> {

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

        Configuration conf = new Configuration();
        conf.set("count", "1");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("err_Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count with sort");

        job.setJarByClass(Chapter05E3WordSortAndPositionDebup.class);
        job.setMapperClass(Chapter05E3WordSortAndPositionMapper.class);
        job.setReducerClass(Chapter05E3WordSortAndPositionReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(Chapter05E3WordSortAndPositionPartition.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        System.err.println("err_input7: " + conf.get("mapreduce.job.inputformat.class"));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
