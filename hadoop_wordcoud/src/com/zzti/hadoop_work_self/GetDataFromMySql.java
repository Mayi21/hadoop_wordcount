package com.zzti.hadoop_work_self;

import DataBase.MySqlServerDao;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class GetDataFromMySql {
	private static Connection connection = MySqlServerDao.getConnection();
	private static String path = "D:\\Study\\JAVA\\idea\\hadoop\\hadoop_wordcoud\\src\\com\\zzti\\FileFolder\\input\\itermScore";
	private static File file = new File(path);
	public static void main(String[] args){
		getAllItermIdFromUserHis();
	}
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
}
