package com.zzti.hadoop_work_self;

import com.zzti.Temp.Recommend;

public class Run {
	public static void main(String[] args) throws Exception{
		UserScoreMatrix.run();
		ItermOccurrence.run();
		TransferUserScore.run();
		RecommendScore.run();
	}
}
