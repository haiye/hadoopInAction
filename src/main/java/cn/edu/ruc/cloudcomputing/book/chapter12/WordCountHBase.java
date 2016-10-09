package cn.edu.ruc.cloudcomputing.book.chapter12;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordCountHBase
{
    // create Map
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		private IntWritable i = new IntWritable(1);
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String s[] =value.toString().trim().split(" ");	//split string using " "
			for( String m : s){
				context.write(new Text(m), i);
			}
		}
	}

	//Create Reduce
	public static class Reduce extends	TableReducer<Text, IntWritable,	NullWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,	InterruptedException{
			int sum = 0;
			for(IntWritable i : values){
				sum += i.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));	//put every key as rowName
			put.add(Bytes.toBytes("cf2"),Bytes.toBytes("count"),Bytes.toBytes(String.valueOf(sum)));//列族为cf2，列修饰符为count，列值为数目
			context.write(NullWritable.get(), put);
		}
	}
	
	
	// create Hbase Table
	public static void createHBaseTable(String tablename)throws IOException{
		HTableDescriptor htd = new	HTableDescriptor(TableName.valueOf(tablename));
		HColumnDescriptor col = new	HColumnDescriptor("cf2");
		htd.addFamily(col);
		
		Configuration config =  HBaseConfiguration.create();;
		HBaseAdmin admin = new HBaseAdmin(config);
		if(admin.tableExists(tablename)){
			System.out.println("table exists, trying recreate table! ");
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
		}
		System.out.println("create new table: " + tablename);
		admin.createTable(htd);
		admin.close();
	}
	
	public static void main(String args[]) throws Exception{
		String tablename = "wordcount";
		Configuration conf = new Configuration();
		conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
		createHBaseTable(tablename);
		String input = args[0];	//get input file
		Job job = Job.getInstance(conf, "WordCount table with " + input);
		job.setJarByClass(WordCountHBase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);	
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
