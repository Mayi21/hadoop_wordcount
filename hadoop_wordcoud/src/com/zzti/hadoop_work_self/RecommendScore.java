package com.zzti.hadoop_work_self;

import Map_Reduce.BaseDriver;
import Map_Reduce.HadoopUtil;
import Map_Reduce.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecommendScore {
	public static class RecommendScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();
		private final static Map<String, List<Iterm>> itermOccurrenceMatrix = new HashMap<String, List<Iterm>>();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
			String[] v1 = tokens[0].split(":");
			String[] v2 = tokens[1].split(":");
			if (v1.length > 1) {
				String itemID1 = v1[0];
				String itemID2 = v1[1];
				int num = Integer.parseInt(tokens[1]);
				List<Iterm> list;
				/**
				 * itermOccurrenceMatrix包含itermID1就把他的value获取，
				 * 并且把这个新获取的添加进去，然后再重新推进去
				 */
				if (!itermOccurrenceMatrix.containsKey(itemID1)) {
					list = new ArrayList<Iterm>();
				} else {
					list = itermOccurrenceMatrix.get(itemID1);
				}
				list.add(new Iterm(itemID1, itemID2, num));
				itermOccurrenceMatrix.put(itemID1, list);
			}
			if (v2.length > 1) {
				// userVector
				String itemID = tokens[0];
				String userID = v2[0];
				double pref = Double.parseDouble(v2[1]);
				k.set(userID);
				for (Iterm co : itermOccurrenceMatrix.get(itemID)) {
					v.set(co.getItemId1() + "," + pref * co.getOccurrence());
					context.write(k, v);
				}
			}
		}
	}
	public static class RecommendScoreReducer extends Reducer<Text,Text,Text,Text>{
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Double> result = new HashMap<String, Double>();
			for (Text value : values) {
				String[] str = value.toString().split(",");
				/**
				 * 如果有这个itermId，就把值取出来再加上
				 * 如果没有itermId，就把值推进去
				 */
				if (result.containsKey(str[0])) {
					result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
				} else {
					result.put(str[0], Double.parseDouble(str[1]));
				}
			}
			for (String itemID : result.keySet()) {
				double score = result.get(itemID);
				v.set(itemID + "," + score);
				context.write(key, v);
			}
		}
	}
	public static void run() throws Exception{
		String inputPath1 = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\output\\TransferUserScore";
		String inputPath2 = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\input\\ItermOccurrence";
		String outPath = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\output\\RecommendScore";
		Configuration configuration = new Configuration();
		JobInitModel recommendJob = new JobInitModel(new String[]{inputPath1, inputPath2}
				, outPath, configuration
				, null
				, "RecommendScore", RecommendScore.class
				, null, RecommendScoreMapper.class, Text.class, Text.class
				, null
				, null, RecommendScoreReducer.class, Text.class, Text.class);
		BaseDriver.initJob(new JobInitModel[]{recommendJob});
	}
}