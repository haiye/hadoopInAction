package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Chapter05E2WordDedup {

    public static class Chapter05E2WordDedupMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new IntWritable());

        }
    }

    public static class Chapter05E2WordDedupReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            context.write(key, new IntWritable());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Chapter05E2WordDedup <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Chapter05E2WordDedup");
        job.setJarByClass(Chapter05E2WordDedup.class);
        job.setMapperClass(Chapter05E2WordDedupMapper.class);
        job.setCombinerClass(Chapter05E2WordDedupReducer.class);
        job.setReducerClass(Chapter05E2WordDedupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
