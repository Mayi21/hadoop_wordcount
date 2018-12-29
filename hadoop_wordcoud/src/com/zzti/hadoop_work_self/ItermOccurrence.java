package com.zzti.hadoop_work_self;

import Map_Reduce.BaseDriver;
import Map_Reduce.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ItermOccurrence {
	public static class ItermOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] toks = value.toString().trim().split("\t");
			String[] va = toks[1].split(":");
			for (int i = 0; i < va.length; i++) {
				for (int j = 0; j < va.length; j++) {
					String com = va[i] + ":" + va[j];
					context.write(new Text(com), new Text("1"));
				}
			}
		}
	}
	public static class ItermOccurrenceReducer extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			int count = 0;
			for (Text value : values){
				count ++;
			}
			context.write(key,new Text(String.valueOf(count)));
		}
	}
	public static void run() throws InterruptedException, IOException, ClassNotFoundException {
		Configuration conf = new Configuration();
		/*TODO 改一下路径*/
		String inPath = "D:\\Study\\JAVA\\idea\\output\\Create_data";
		String outPath = "D:\\Study\\JAVA\\idea\\output\\AdjacencyMatrix";
		JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "ItermOccurrence", ItermOccurrence.class
				, null, ItermOccurrenceMapper.class, Text.class, Text.class, null, null
				, ItermOccurrenceReducer.class, Text.class, Text.class);
		BaseDriver.initJob(new JobInitModel[]{job});
	}
}
