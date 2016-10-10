package cn.edu.ruc.cloudcomputing.book.chapter03;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountOldReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException {
        /* sum =0 below if we using value_bak = values; value_bak.hasNext() */
        // Iterator<IntWritable> value_bak = values;
        // System.out.println("Reduce_debug: input_key = " + key);
        // int i = 0;
        // while (value_bak.hasNext()) {
        // System.out.println("Reduce_debug: i = " + (i++) +
        // "; input_value = " + values.next().get());
        // }
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
            System.out.println("Reduce_debug: sum = " + sum);
        }
        System.out.println("Reduce_debug: key = " + key + "; sum = " + sum);

        output.collect(key, new IntWritable(sum));
    }

}
