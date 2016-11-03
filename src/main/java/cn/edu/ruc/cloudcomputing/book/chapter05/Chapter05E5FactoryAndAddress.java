package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Chapter05E5FactoryAndAddress {
    public static int time = 0;

    public static class Chapter05E5FactoryAndAddressMap extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String lineContent = value.toString();
            if (lineContent.contains("factoryname") == true || lineContent.contains("addressID") == true) {
                return;
            }

            String regex = "[1-9]";

            String firstChar = lineContent.substring(0, 1);
            if (firstChar.matches(regex)) {
                System.out.println("this line containers addrID and addrName; line_content=" + lineContent);

                // write data into left table <addressId, 1+addressName>

                // 不能用lineContent.substring(0,1)，因为addrId可能是两位数
                String addrId = lineContent.substring(0, lineContent.indexOf(' '));
                String addrName = lineContent.substring(lineContent.indexOf(' ') + 1);
                System.out.println("addrId=" + addrId + " addrName = " + addrName);
                context.write(new Text(addrId), new Text("1+" + addrName));
            } else {
                System.out.println("this line containers factoryName amd addrID; line_content=" + lineContent);

                // write data into right table <addressId, 2+factoryName>

                String addrId = lineContent.substring(lineContent.lastIndexOf(' ') + 1);
                String factoryName = lineContent.substring(0, lineContent.lastIndexOf(' '));
                System.out.println("addrId=" + addrId + " factoryName = " + factoryName);
                context.write(new Text(addrId), new Text("2+" + factoryName));
            }
        }
    }

    public static class Chapter05E5FactoryAndAddressReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> iterator = values.iterator();

            System.out.println("this key = " + key);
            List<String> addressNameList = new ArrayList<String>();
            List<String> factoryNameList = new ArrayList<String>();
            while (iterator.hasNext()) {
                String record = iterator.next().toString();
                String type = record.substring(0, record.indexOf("+"));
                if (type.equals("1")) {
                    String addressName = record.substring(record.indexOf("+") + 1);
                    System.out.println("this record containers addressName; record_content = " + record
                            + "; addressName = " + addressName);

                    addressNameList.add(addressName);
                } else {
                    String factoryName = record.substring(record.indexOf("+") + 1);
                    System.out.println("this record containers factoryName; record_content = " + record
                            + "; factoryName = " + factoryName);

                    factoryNameList.add(factoryName);
                }
            }

            String[] addrArrays = addressNameList.toArray(new String[0]);
            System.out.println("addrArrays =" + Arrays.toString(addrArrays));

            String[] factoryArrays = factoryNameList.toArray(new String[0]);
            System.out.println("factoryArrays =" + Arrays.toString(factoryArrays));

            for (String factory : factoryArrays) {
                for (String addr : addrArrays) {
                    System.out.println("factory = " + factory + " addr = " + addr);
                    context.write(new Text(factory), new Text(addr));

                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "multiple table join");
        job.setJarByClass(Chapter05E5FactoryAndAddress.class);
        job.setMapperClass(Chapter05E5FactoryAndAddressMap.class);
        job.setReducerClass(Chapter05E5FactoryAndAddressReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
