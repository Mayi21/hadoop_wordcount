package com.zzti.hadoop_work_self;

import DataBase.MySqlServerDao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class UploadDataToMysql {
	private static String path = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\output\\RecommendScore\\part-r-00000";
	private static Connection connection = MySqlServerDao.getConnection();
	private static Pattern pattern = Pattern.compile("[\t,]");
	public static void uploadInfoToMySql(){
		File file = new File(path);
		List<String> list = new ArrayList<>();
		BufferedReader bufferedReader = null;
		try{
			bufferedReader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = bufferedReader.readLine()) != null){
				list.add(line);
			}
			/**
			 * 将生成的评分，写入到数据库中
			 */
			for (String s: list){
				String[] info = pattern.split(s);
				String id = String.valueOf(System.currentTimeMillis());
				String userId = info[0];
				String itermId = info[1];
				float preference = Float.parseFloat(info[2]);
				String sql = "INSERT INTO showIndex (id,userId,itermId,preference) value (?,?,?,?)";
				PreparedStatement preparedStatement = connection.prepareStatement(sql);
				preparedStatement.setString(1,id);
				preparedStatement.setString(2,userId);
				preparedStatement.setString(3,itermId);
				preparedStatement.setFloat(4,preference);
				preparedStatement.executeUpdate();
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
		System.out.println("-----------------");
	}
	/**
	 * 从showIndex中按照一定顺序获得itermId
	 * 同时获取该itermId的相关信息
	 */
	public static List<ItermInfo> getDesFromSql(String userId){
		List<ItermInfo> list = new ArrayList<ItermInfo>();
		String sql = "select itermId from showIndex where userId =" + userId + " order by preference desc";
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				String itermId = resultSet.getString("itermId");
				sql = "select * from iterm where itermId=" + itermId;
				resultSet = statement.executeQuery(sql);
				ItermInfo itermInfo = null;
				while (resultSet.next()){
					itermInfo = new ItermInfo();
					itermInfo.setItermId(resultSet.getString("itermId"));
					itermInfo.setItermTag(resultSet.getString("itermTag"));
					itermInfo.setItermName(resultSet.getString("itermName"));
					itermInfo.setItermImg("https://obs-da.obs.cn-north-1.myhwclouds.com/jingdong/" + resultSet.getString("itermId") + "/" + resultSet.getString("itermId") + ".jpg");
					list.add(itermInfo);
				}
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
		return list;
	}
}