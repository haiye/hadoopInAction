package cn.edu.ruc.cloudcomputing.book.chapter09;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat2 {

 
    public static void main(String[] args) {
        String uri=args[0];
        Configuration conf = new Configuration();
        FSDataInputStream in=null;
        try {
            FileSystem fs=  FileSystem.get(URI.create(uri), conf);
            in=fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096,false);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }
    }

}
