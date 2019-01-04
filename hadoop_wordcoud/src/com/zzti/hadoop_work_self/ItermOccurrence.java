package com.zzti.hadoop_work_self;

import Map_Reduce.BaseDriver;
import Map_Reduce.HadoopUtil;
import Map_Reduce.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ItermOccurrence {
	public static class ItermOccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		Text k = new Text();
		IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//输入的数据格式为:1	103:2.5,101:5.0,102:3.0
			String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
			//提取每行的itermId进行全排列输出
			for (int i = 1; i < strArr.length; i++) {
				String itermId1 = strArr[i].split(":")[0];
				for (int j = 1; j < strArr.length; j++) {
					String itermId2 = strArr[j].split(":")[0];
					k.set(itermId1 + ":" + itermId2);
					context.write(k, one);
				}
			}
		}
	}
	public static class ItermOccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable resCount = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			resCount.set(sum);
			context.write(key, resCount);
		}
	}
	public static void run() throws InterruptedException, IOException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String inPath = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\input\\UserScoreMatrix";
		String outPath = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\input\\ItermOccurrence";
		JobInitModel itermOccurrenceJob = new JobInitModel(new String[]{inPath}, outPath, conf
				, null
				, "ItermOccurrence", ItermOccurrence.class
				, null, ItermOccurrenceMapper.class, Text.class, IntWritable.class
				, null
				, null, ItermOccurrenceReducer.class, Text.class, IntWritable.class);
		BaseDriver.initJob(new JobInitModel[]{itermOccurrenceJob});
	}
}
