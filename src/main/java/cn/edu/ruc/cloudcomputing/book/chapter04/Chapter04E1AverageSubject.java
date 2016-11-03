package cn.edu.ruc.cloudcomputing.book.chapter04;

/**
 * get average score for all subjects
 * 
 * */
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Chapter04E1AverageSubject extends Configured implements Tool {
    static int static_int_map = 1;
    static int static_int_reduce = 1;

    public static class Chapter04E1AverageSubjectMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int SUBJECT_COUNT = 1;

            System.out.println("WordCountChapter04E1Map_debug: mapTask number=" + (static_int_map++));

            String line = value.toString(); // 将输入的纯文本文件的数据转化成String
            System.out.println(line);// 为了便于程序的调试，输出读入的内容
            // 将输入的数据先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            // 分别对每一行进行处理
            while (tokenizerArticle.hasMoreTokens()) {

                String next_str = tokenizerArticle.nextToken();

                // 每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(next_str);
                String nameStr = tokenizerLine.nextToken(); // 学生姓名部分
                Text nameText = new Text(nameStr);// 学生姓名

                String scoreStr = tokenizerLine.nextToken();// 成绩部分
                scoreStr = scoreStr + "~" + SUBJECT_COUNT;// scoreOfStudent~studentCount
                Text scoreText = new Text(scoreStr);

                System.out.println("student=" + nameText + "; score=" + scoreText);

                context.write(nameText, scoreText);// 输出姓名和成绩
            }
        }
    }

    public static class Chapter04E1AverageSubjectCombine extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("WordCountChapter04E1Combine_debug: student_name=" + key.toString());

            int sum = 0;
            int subjectCounts = 0;

            Iterator<Text> iterator = values.iterator();

            while (iterator.hasNext()) {
                String scoreAndSubjectCounts = iterator.next().toString();
                StringTokenizer scoreAndSubjectCountsStringTokenizer = new StringTokenizer(scoreAndSubjectCounts, "~");
                while (scoreAndSubjectCountsStringTokenizer.hasMoreTokens()) {
                    String scoreStr = scoreAndSubjectCountsStringTokenizer.nextToken();
                    String subjectCountsStr = scoreAndSubjectCountsStringTokenizer.nextToken();
                    sum += Integer.parseInt(scoreStr);
                    subjectCounts += Integer.parseInt(subjectCountsStr);
                    System.out.println("scoreStr=" + scoreStr + "; subjectCounts=" + subjectCounts + "; sum =" + sum);
                }
            }

            /* main difference between combine and reduce */
            context.write(key, new Text(sum + "~" + subjectCounts));
        }
    }

    public static class Chapter04E1AverageSubjectReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("WordCountChapter04E1Reduce_debug: reduceTask number=" + (static_int_reduce++));

            int sum = 0;
            int subjectCounts = 0;

            Iterator<Text> iterator = values.iterator();

            while (iterator.hasNext()) {
                String scoreAndSubjectCounts = iterator.next().toString();
                StringTokenizer scoreAndSubjectCountsStringTokenizer = new StringTokenizer(scoreAndSubjectCounts, "~");
                while (scoreAndSubjectCountsStringTokenizer.hasMoreTokens()) {
                    String scoreStr = scoreAndSubjectCountsStringTokenizer.nextToken();
                    String subjectCountsStr = scoreAndSubjectCountsStringTokenizer.nextToken();
                    sum += Integer.parseInt(scoreStr);
                    subjectCounts += Integer.parseInt(subjectCountsStr);
                    System.out.println("scoreStr=" + scoreStr + "; subjectCounts=" + subjectCounts + "; sum =" + sum);
                }
            }

            /* main difference between combine and reduce */
            int average = sum / subjectCounts;// 计算平均成绩
            context.write(key, new Text(average + ""));

        }
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Chapter04E1AverageSubject.class);
        job.setJobName("WordCountChapter04E1");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(Chapter04E1AverageSubjectMap.class);

        job.setCombinerClass(Chapter04E1AverageSubjectCombine.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(Chapter04E1AverageSubjectReduce.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Chapter04E1AverageSubject(), args);
        System.exit(ret);
    }
}
