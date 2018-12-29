package com.zzti.hadoop_work_self;

import Map_Reduce.AdjacencyMatrix;
import Map_Reduce.BaseDriver;
import Map_Reduce.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserScoreMatrix {
	public static class UserScoreMatrixMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try{
				System.load("D:\\DOWNLOAD\\hadoop-2.8.5\\bin");
			}catch (Throwable t){
			}
			String[] toks = value.toString().trim().split("\t");
			for (int i = 1;i < toks.length;i++){
				context.write(new Text(String.valueOf(i)),new Text(toks[0] + ":" + toks[i]));
			}
		}
	}
	public static class UserScoreMatrixReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String v = null;
			for (Text value : values){
				v += value.toString() + ",";
			}
			context.write(key,new Text(v));
		}
	}

	public static void run() throws InterruptedException, IOException, ClassNotFoundException {
		Configuration conf = new Configuration();
		/* TODO 输入路径改一下*/
		String inPath = "D:\\Study\\JAVA\\idea\\output\\Create_data";
		String outPath = "D:\\Study\\JAVA\\idea\\output\\AdjacencyMatrix";
		JobInitModel userScoreMatrixJob = new JobInitModel(new String[]{inPath}, outPath, conf
				, null
				, "UserScoreMatrix", UserScoreMatrix.class
				, null, UserScoreMatrixMapper.class, Text.class, Text.class
				, null
				, null,UserScoreMatrixReducer.class, Text.class, Text.class);
		BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob});
	}
}