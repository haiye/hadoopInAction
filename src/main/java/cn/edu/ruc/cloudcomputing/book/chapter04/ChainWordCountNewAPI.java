package cn.edu.ruc.cloudcomputing.book.chapter04;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainWordCountNewAPI extends Configured implements Tool {

    public static class FilterMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

        private final static String[] stopWord = { "a", "an", "the", "of", "in", "and", "to", "at", "with", "as", "for" };
        private HashSet<String> stopWordSet;

        @Override
        public void setup(Context context) {
            stopWordSet = new HashSet<String>();
            for (int i = 0; i < stopWord.length; i++) {
                stopWordSet.add(stopWord[i]);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String aWord = tokenizer.nextToken();
                if (stopWordSet.contains(aWord) == true) {
                    continue;
                }
                context.write(new Text(aWord), new Text(""));

            }
        }

    }

    public static class CounterMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, one);
        }
    }

    public class IntSumReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String jobName = "Hadoop Chapt04 ChainMapper&&ChainReducer";
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(ChainWordCountNewAPI.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // add FilterMapper into this job, who would filt unnecessary words
        Configuration map1Conf = new Configuration(false);
        ChainMapper
                .addMapper(job, FilterMapper.class, LongWritable.class, Text.class, Text.class, Text.class, map1Conf);

        // add CounterMapper into this job, who would take words count
        Configuration map2Conf = new Configuration(false);
        ChainMapper.addMapper(job, CounterMapper.class, LongWritable.class, Text.class, Text.class, Text.class,
                map2Conf);

        // add Reduce into this job, who would take words count
        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, IntSumReducer.class, LongWritable.class, Text.class, Text.class, Text.class,
                reduceConf);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int ret = ToolRunner.run(new ChainWordCountNewAPI(), args);
        System.exit(ret);

    }

}
