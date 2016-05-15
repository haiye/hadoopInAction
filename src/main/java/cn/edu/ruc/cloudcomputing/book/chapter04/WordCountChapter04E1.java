package cn.edu.ruc.cloudcomputing.book.chapter04;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCountChapter04E1 extends Configured implements Tool {
	static int static_int_map = 1;
	static int static_int_reduce = 1;

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			System.out.println("yehaimin_debug: mapTask number=" + static_int_map);
			static_int_reduce++;
			String line = value.toString(); // 将输入的纯文本文件的数据转化成String
			System.out.println(line);// 为了便于程序的调试，输出读入的内容
			// 将输入的数据先按行进行分割
			StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
			// 分别对每一行进行处理
			while (tokenizerArticle.hasMoreTokens()) {
				// 每行按空格划分
				StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
				String strName = tokenizerLine.nextToken(); // 学生姓名部分
				String strScore = tokenizerLine.nextToken();// 成绩部分
				Text name = new Text(strName);// 学生姓名
				int scoreInt = Integer.parseInt(strScore);// 学生成绩score of
															// student
				context.write(name, new IntWritable(scoreInt));// 输出姓名和成绩
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			System.out.println("yehaimin_debug: reduceTask number=" + static_int_reduce);
			static_int_reduce++;
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get(); // 计算总分
				count++;// 统计总的科目数
			}
			int average = (int) sum / count;// 计算平均成绩
			context.write(key, new IntWritable(average));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(getConf());
		job.setJarByClass(WordCountChapter04E1.class);
		job.setJobName("Score_Process");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new WordCountChapter04E1(), args);
		System.exit(ret);
	}
}
