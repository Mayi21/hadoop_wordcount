package com.zzti.hadoop_work_self;

import DataBase.MySqlServerDao;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

public class GetDataFromMySql {
	private static Connection connection = MySqlServerDao.getConnection();
	private static String path = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\input\\itermScore";
	private static File file = new File(path);
	public static void getAllItermIdFromUserHis(){
		String sql = "select itermId from userhis";
		Set set = new TreeSet();
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				set.add(resultSet.getString("itermId"));
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
		Iterator<String> iterator = set.iterator();
		while (iterator.hasNext()){
			String itermId = iterator.next();
			writeInfo(itermId);
		}
	}
	/**
	 * @deprecated 把分数写入文件中
	 * @param itermId
	 * @return null
	 */
	public static void writeInfo(String itermId){
		TreeMap<String,Integer> treeMap = getUserIdMap();
		Iterator<String> iterator = treeMap.keySet().iterator();
		String string = itermId;
		while (iterator.hasNext()){
			String key = iterator.next();
			String preference = getItermPreference(itermId,key);
			string += "\t" + preference;
		}
		string += "\n";
		try {
			BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file,true));
			bufferedOutputStream.write(string.getBytes());
			bufferedOutputStream.close();
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
	}
	/**
	 * @deprecated 得到注册用户的itermId
	 * @param null
	 * @return TreeMap<String,Integer>
	 */
	public static TreeMap<String,Integer> getUserIdMap(){
		String sql = "select userId from userinfo";
		TreeMap<String,Integer> treeMap = new TreeMap<String,Integer>();
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				String userId = resultSet.getString("userId");
				treeMap.put(userId,1);
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
		return treeMap;
	}
	/**
	 * @deprecated 得到对应物品和用户的评分
	 * @param itermId,userId
	 * @return preference
	 */
	public static String getItermPreference(String itermId,String userId){
		String sql = "select * from userhis where itermId =" + itermId + " and userId=" + userId;
		String preference = null;
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				preference = resultSet.getString("preference");
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
		if (preference	!= null){
			return preference;
		}else {
			return "0";
		}
	}
	/**
	 * @deprecated 上传mr后的结果到showIndex表中
	 * @param null
	 * @retuen void
	 */
	public static void uploadInfoToMySql(){
		Pattern pattern = Pattern.compile("[\t,]");
		Connection connection = MySqlServerDao.getConnection();
		String path = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\output\\RecommendScore\\part-r-00000";
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
	}
}
