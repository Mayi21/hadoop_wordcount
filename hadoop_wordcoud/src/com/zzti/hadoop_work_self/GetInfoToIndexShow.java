package com.zzti.hadoop_work_self;

import DataBase.MySqlServerDao;
import org.apache.zookeeper.data.Stat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class GetInfoToIndexShow {
	private static Connection connection = MySqlServerDao.getConnection();
	public static List<ItermInfo> getDesFromSql(String userId){
		List<ItermInfo> list = new ArrayList<ItermInfo>();
		String sql = "select itermId from showIndex where userId =" + userId + " order by preference desc";
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				String itermId = resultSet.getString("itermId");
				String sql1 = "select * from iterm where itermId=" + itermId;
				ResultSet resultSet1 = connection.createStatement().executeQuery(sql1);
				ItermInfo itermInfo = null;
				while (resultSet1.next()) {
					itermInfo = new ItermInfo();
					itermInfo.setItermId(resultSet1.getString("itermId"));
					itermInfo.setItermTag(resultSet1.getString("itermTag"));
					itermInfo.setItermName(resultSet1.getString("itermName"));
					itermInfo.setItermImg("https://obs-da.obs.cn-north-1.myhwclouds.com/jingdong/" + resultSet1.getString("itermId") + "/" + resultSet1.getString("itermId") + ".jpg");

					list.add(itermInfo);
				}
			}
		}catch (Exception e){
			System.out.println("第一个" + e.getMessage());
		}
		return list;
	}
}
