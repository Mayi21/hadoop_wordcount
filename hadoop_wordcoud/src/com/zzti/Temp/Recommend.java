package com.zzti.Temp;

import DataBase.MySqlServerDao;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Recommend {
	private static String path = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\output\\RecommendScore\\part-r-00000";
	private static Connection connection = MySqlServerDao.getConnection();
	private static Pattern pattern = Pattern.compile("[\t,]");
	public static void main(String[] args) throws Exception{
		//uploadInfoToMySql();
		getDesFromSql();
	}
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
	public static void getDesFromSql(){
		String sql = "select itermId,preference from showIndex where userId = '1' order by preference desc";
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				System.out.println(resultSet.getString("itermId") + " " + resultSet.getString("preference"));
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
	}
}