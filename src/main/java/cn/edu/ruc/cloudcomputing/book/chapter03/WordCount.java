package cn.edu.ruc.cloudcomputing.book.chapter03;
/*
 * It works well after test on horton environment
 * */
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

    public static class WordCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            System.out.println("Map_debug: key = " + key + "; value = " + value);
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                System.out.println("Map_debug: word = " + word);
                output.collect(word, one);
            }
        }
    }

    public static class WordCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {
/* sum =0 below if we using value_bak = values; value_bak.hasNext() */
//            Iterator<IntWritable> value_bak = values;
//            System.out.println("Reduce_debug: input_key = " + key);
//            int i = 0;
//            while (value_bak.hasNext()) {
//                System.out.println("Reduce_debug: i = " + (i++) + "; input_value = " + values.next().get());
//            }
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                System.out.println("Reduce_debug: sum = " + sum);
            }
            System.out.println("Reduce_debug: key = " + key + "; sum = " + sum);

            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("Hadoop Chapt03 wordcount");

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        conf.setInputFormat(TextInputFormat.class);

        conf.setMapperClass(WordCountMap.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setReducerClass(WordCountReduce.class);

        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);
    }
}
