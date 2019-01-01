package com.zzti.hadoop_work_self;

import Map_Reduce.BaseDriver;
import Map_Reduce.HadoopUtil;
import Map_Reduce.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
/**
 * @author May
 * @date 2018/12/29
 */
public class TransferUserScore {
	public static class TransferUserScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
			for (int i = 1; i < strArr.length; i++) {
				k.set(strArr[i].split(":")[0]);
				v.set(strArr[0] + ":" + strArr[i].split(":")[1]);
				context.write(k, v);
			}
		}
	}
	public static void run() throws IOException,InterruptedException,ClassNotFoundException{
		/* TODO 输入路径*/
		String inputPath = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\input\\UserScoreMatrix";
		/* TODO 输出路径*/
		String outPath = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\output\\TransferUserScore";
		Configuration configuration = new Configuration();
		JobInitModel transferUserScoreJob = new JobInitModel(new String[]{inputPath}, outPath
				, configuration
				, null
				, "TransferUserScore", TransferUserScore.class
				, null, TransferUserScoreMapper.class, Text.class, Text.class
				, null
				, null
				, null
				, null
				, null

		);
		BaseDriver.initJob(new JobInitModel[]{transferUserScoreJob});

	}
}