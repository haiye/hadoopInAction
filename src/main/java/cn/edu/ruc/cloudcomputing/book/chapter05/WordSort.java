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
 * NOT rename this java file as Sort.
 * it would run default Sort file instead of this one 
 * 
 * **/


public class WordSort {
    
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable data= new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String line=value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable lineNum= new IntWritable(1);
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
           
            for(IntWritable value:values){
                context.write(lineNum, key);
                lineNum=new IntWritable(lineNum.get()+1);
            }
        }
    }
    public static class Partition extends Partitioner <IntWritable,IntWritable>{

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int Maxnumber=65523;
            int bound=Maxnumber/numPartitions+1;
            int keynumber=key.get();
            for(int i=0; i<numPartitions;i++){
                if(keynumber< bound* (i+1) && keynumber>= bound*i){
                    return i;
                }
            }
            
            
            return -1;
        }
        
    }

    public static void main(String[] args) throws Exception {
        
        Log LOG =LogFactory.getLog(WordSort.class);
        Configuration conf = new Configuration();
        System.out.println("out_input1: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input1: " + conf.get("mapreduce.job.inputformat.class"));
        LOG.info("info_input1: " + conf.get("mapreduce.job.inputformat.class"));

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("err_Usage: wordcount <in> <out>");
            System.exit(2);
        }
        LOG.info("info_input2: " + conf.get("mapreduce.job.inputformat.class"));
        System.out.println("out_input2: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input2: " + conf.get("mapreduce.job.inputformat.class"));

        Job job = Job.getInstance(conf, "word count with sort");
        
        LOG.info("info_input3: " + conf.get("mapreduce.job.inputformat.class"));
        System.out.println("out_input3: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input3: " + conf.get("mapreduce.job.inputformat.class"));

        job.setJarByClass(WordSort.class);
        job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        System.out.println("out_input4: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input4: " + conf.get("mapreduce.job.inputformat.class"));

        LOG.info("info_input4: " + conf.get("mapreduce.job.inputformat.class"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("out_input5: " + conf.get("mapreduce.job.inputformat.class"));
        LOG.info("info_input5: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input5: " + conf.get("mapreduce.job.inputformat.class"));

        job.setPartitionerClass(Partition.class);
        System.out.println("out_input6: " + conf.get("mapreduce.job.inputformat.class"));
        LOG.info("info_input6: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input6: " + conf.get("mapreduce.job.inputformat.class"));


        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("out_input7: " + conf.get("mapreduce.job.inputformat.class"));
        LOG.info("info_input7: " + conf.get("mapreduce.job.inputformat.class"));
        System.err.println("err_input7: " + conf.get("mapreduce.job.inputformat.class"));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
