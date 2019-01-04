package com.zzti.hadoop_work_self;

import DataBase.MySqlServerDao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ConcurrentModificationException;
import java.util.List;

public class Temp {
	public static void main(String[] args) throws Exception{
		/*
		UserScoreMatrix.run();
		ItermOccurrence.run();
		TransferUserScore.run();
		RecommendScore.run();
		*/
		List<ItermInfo> list = GetInfoToIndexShow.getDesFromSql("1");
		for (int i = 0;i < list.size();i++){
			System.out.println(list.get(i).getItermId());
		}

	}
}
