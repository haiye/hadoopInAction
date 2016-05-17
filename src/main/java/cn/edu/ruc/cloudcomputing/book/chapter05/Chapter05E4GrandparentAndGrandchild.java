package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Chapter05E4GrandparentAndGrandchild {
    public static int time = 0;

    public static class Chapter05E4GrandparentAndGrandchildMap extends Mapper<Object, Text, Text, Text> {
        // map将输入分割成child和parent，然后正序输出一次作为右表，反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表区别标志
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String childname = new String();
            String parentname = new String();
            String relationtype = new String();
            String line = value.toString();
            int i = 0;
            System.out.println("key="+key.toString());
            System.out.println("value="+key.toString());
            while (line.charAt(i) != ' ') {
                i++;
            }
            String[] values = { line.substring(0, i), line.substring(i + 1) };
            if (values[0].compareTo("child") != 0) {
                childname = values[0];
                parentname = values[1];
                relationtype = "1"; // 左右表区分标志־
                context.write(new Text(values[1]), new Text(relationtype + "+" + childname + "+" + parentname));

                // 左表
                relationtype = "2";
                context.write(new Text(values[0]), new Text(relationtype + "+" + childname + "+" + parentname));
                // 右表
            }
            System.out.println("context="+context.toString());
            System.out.println("context.getCurrentKey="+context.getCurrentKey());
            System.out.println("context.getCurrentValue="+context.getCurrentValue());
            
            System.out.println("context.getOutputKeyClass="+context.getOutputKeyClass());
            Class<Text> outputKey=(Class<Text>) context.getOutputKeyClass();
            System.out.println("context.outputKey="+outputKey.toString());
            
            System.out.println("context.getOutputValueClass="+context.getOutputValueClass());
            Class<Text> outputValue=(Class<Text>) context.getOutputValueClass();
            System.out.println("outputValue.outputKey="+outputValue.toString());

        }
    }

    public static class Chapter05E4GrandparentAndGrandchildReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if (time == 0) { // 输出表头
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            int grandchildnum = 0;
            String grandchild[] = new String[10];
            int grandparentnum = 0;
            String grandparent[] = new String[10];
            Iterator ite = values.iterator();
            while (ite.hasNext()) {
                String record = ite.next().toString();
                int len = record.length();
                int i = 2;
                if (len == 0)
                    continue;
                char relationtype = record.charAt(0);
                String childname = new String();
                String parentname = new String();
                // 获取value-list中value的child
                while (record.charAt(i) != '+') {
                    childname = childname + record.charAt(i);
                    i++;
                }
                i = i + 1;
                // 获取value-list中value的parent
                while (i < len) {
                    parentname = parentname + record.charAt(i);
                    i++;
                }
                // 左表，取出child放入grandchild
                if (relationtype == '1') {
                    grandchild[grandchildnum] = childname;
                    grandchildnum++;
                } else {
                    grandparent[grandparentnum] = parentname;
                    grandparentnum++;
                }
            }
            // 右表，取出parent放入grandparent
            if (grandparentnum != 0 && grandchildnum != 0) {
                for (int m = 0; m < grandchildnum; m++) {
                    for (int n = 0; n < grandparentnum; n++) {
                        context.write(new Text(grandchild[m]), new Text(grandparent[n])); // ������
                    }
                }
            }

        }
    }

    //right Command: hadoop jar hadoopInAction-0.0.4-SNAPSHOT.jar cn.edu.ruc.cloudcomputing.book.chapter05.STjoin -Dmapred.job.queue.name=risk_platform child_parent.txt output_child12
    //wrong Command: hadoop jar hadoopInAction-0.0.4-SNAPSHOT.jar cn.edu.ruc.cloudcomputing.book.chapter05.STjoin child_parent.txt output_child12 -Dmapred.job.queue.name=risk_platform
    //Reason: place -Dmapred.job.queue.name=risk_platform before the "child_parent.txt output_child12", or mapred.job.queue.name=risk_platform couldn't be parsed
    // in the right command, otherArgs[0] still is child_parent.txt even it's in the second position of parameter list, and otherArgs[1] is still output_child12 even it's in the third position of parameter list
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        for(String arg: args){
            System.out.println("arg_input="+arg);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        for(String arg: otherArgs){
            System.out.println("arg_GenericOptionsParser="+arg);
        }
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        System.out.println("queueName="+conf.get("mapred.job.queue.name"));
        conf.set("mapred.job.queue.name", "risk_platform");
        System.out.println("queueName_afterset="+conf.get("mapred.job.queue.name"));

        Job job = Job.getInstance(conf, "single table join");
        job.setJarByClass(Chapter05E4GrandparentAndGrandchild.class);
        job.setMapperClass(Chapter05E4GrandparentAndGrandchildMap.class);
        job.setReducerClass(Chapter05E4GrandparentAndGrandchildReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
