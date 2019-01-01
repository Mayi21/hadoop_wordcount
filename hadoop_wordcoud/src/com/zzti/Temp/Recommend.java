package com.zzti.Temp;

import Map_Reduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Recommend {
	public class RecommendMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		Text k = new Text();
		DoubleWritable v = new DoubleWritable();
		/**
		* 第二个Map存储的是同现矩阵列方向上的itermId和对应的同现度
		*/
		Map<String, Map<String, Double>> colItermOccurrenceMap = new HashMap<String, Map<String, Double>>();

		/**
		 * 读取分布式缓存中的同现矩阵进行初始化操作
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				/**
				 * 使用过时的getLocalCacheFiles方法,使用通过symlink访问失败,提示找不到该文件,但是链接已经生成了
				 * 可能性1:当前程序执行路径不对
				 * 可能性2:伪分布式集群有兼容性问题
				 * 测试symlink使用的路径:itermOccurrenceMatrix  ./itermOccurrenceMatrix
				 */
				String path = context.getLocalCacheFiles()[0].getName();
				File itermOccurrenceMatrix = new File(path);
				FileReader fileReader = new FileReader(itermOccurrenceMatrix);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String s;
				//读取文件的每一行
				while ((s = bufferedReader.readLine()) != null) {
					String[] strArr = HadoopUtil.SPARATOR.split(s);
					String[] itermIds = strArr[0].split(":");
					String itermId1 = itermIds[0];
					String itermId2 = itermIds[1];
					Double perference = Double.parseDouble(strArr[1]);
					Map<String, Double> colItermMap;
					if (!colItermOccurrenceMap.containsKey(itermId1)) {
						colItermMap = new HashMap<String, Double>();
					} else {
						colItermMap = colItermOccurrenceMap.get(itermId1);
					}
					colItermMap.put(itermId2, perference);
					colItermOccurrenceMap.put(itermId1, colItermMap);
				}
				bufferedReader.close();
				fileReader.close();
			}
		}

		/**
		 * 读取初始化后的map(同现矩阵),根据用户的评分记录来查找计算对应物品的喜好度
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
			String[] firstStr = strArr[0].split(":");
			//开始计算该用户对各个物品的喜好度
			String userId = firstStr[0];
			//循环物品同现矩阵的行,计算各个物品
			for (Map.Entry<String, Map<String, Double>> rowEntry : colItermOccurrenceMap.entrySet()) {
				//要计算用户对其喜好度的itermId
				String targetItermId = rowEntry.getKey();
				//如果该物品已经被该用户评过分,说明该用户已经看过该物品了,跳过
				if (value.toString().contains(targetItermId)) {
					continue;
				}
				//计算得到的总得分
				Double totalScore = 0.0;
				//存储着该targetItermId对应同现矩阵上的每一列
				Map<String, Double> colIterMap = rowEntry.getValue();

				for (int i = 1; i < strArr.length; i++) {
					String[] itermPer = strArr[i].split(":");
					//同现矩阵上列方向的ItermId
					String itermId2 = itermPer[0];
					Double perference = Double.parseDouble(itermPer[1]);

					Double occurrence = 0.0;
					//如果同现矩阵中没有该物品,那么说明当前两个物品相似度为0
					if (colIterMap.get(itermId2) != null) {
						occurrence = colIterMap.get(itermId2);
					}
					Double score = perference * occurrence;
					totalScore += score;
				}
				k.set(userId + ":" + targetItermId);
				v.set(totalScore);
				context.write(k, v);
			}
		}
	}

	public class RecommendReducer extends Reducer<Text, DoubleWritable, Text, Text> {
		Text userId = new Text();
		Text itermScore = new Text();

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			Double totalScore = 0.0;
			for (DoubleWritable v : values) {
				totalScore += v.get();
			}
			String[] strArr = key.toString().split(":");
			userId.set(strArr[0]);
			itermScore.set(strArr[1] + ":" + totalScore);
			context.write(userId, itermScore);
		}
	}

	public static void run() throws InterruptedException, IOException, ClassNotFoundException {
		Configuration conf = new Configuration();
		/*TODO 改路径*/
		String inPath1 = "D:\\Study\\JAVA\\idea\\output\\AdjacencyMatrix";
		String inPath2 = "D:\\Study\\JAVA\\idea\\output\\Create_pr";
		String outPath = "D:\\Study\\JAVA\\idea\\output\\CalcPeopleRank";
		JobInitModel job = new JobInitModel(new String[]{inPath1, inPath2},
				outPath,
				conf,
				null,
				"Recommend",
				Recommend.class,
				null,
				RecommendMapper.class,
				Text.class,
				Text.class,
				null,
				null,
				RecommendReducer.class,
				Text.class,
				Text.class);
		BaseDriver.initJob(new JobInitModel[]{job});
		File_op.Delete(inPath2);
		File_op.Rname(outPath, inPath2);
		File_op.Rname(inPath2 + "\\part-r-00000", inPath2 + "\\peoplerank");
	}
}