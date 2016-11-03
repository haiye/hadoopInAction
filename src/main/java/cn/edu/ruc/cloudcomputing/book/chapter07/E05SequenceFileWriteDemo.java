package cn.edu.ruc.cloudcomputing.book.chapter07;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class E05SequenceFileWriteDemo {
    private static String[] myValue = { "hello world", "bye world", "hello hadoop", "bye hadoop" };

    public static void main(String[] args) throws IOException {
        String uri = "test_file3";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(), CompressionType.BLOCK);//
            for (int i = 0; i < 5000000; i++) {
                key.set(5000000 - i);
                value.set(myValue[i % myValue.length]);
                if (i < 1000)
                    System.out.println("key=" + key.get() + "value=" + myValue[i % myValue.length]);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
