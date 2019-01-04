package com.zzti.hadoop_work_self;

import DataBase.MySqlServerDao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * 产生模拟数据
 * 上传到userhis
 */
public class SimulationData {
	private static Connection connection = MySqlServerDao.getConnection();
	private static Random random = new Random();
	public static void dataToMysql(){
		System.out.println("dataToMysql");
		Map<String,Map<String,String>> infos = getData();
		String sql = "insert into userhis (id,userId,itermId,preference) values (?,?,?,?)";
		int i = 0;
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			Iterator<String> iterators = infos.keySet().iterator();
			while (iterators.hasNext()){
				String userId = iterators.next();
				Map<String,String> info = infos.get(userId);
				Iterator<String> iterator = info.keySet().iterator();
				while (iterator.hasNext()){
					i++;
					String id = String.valueOf(System.currentTimeMillis());
					Thread.sleep(1);
					String itermId = iterator.next();
					String score = info.get(itermId);

					preparedStatement.setString(1,id);
					preparedStatement.setString(2,userId);
					preparedStatement.setString(3,itermId);
					preparedStatement.setString(4,score);

					preparedStatement.executeUpdate();
					System.out.println("第" + i + "条记录");
				}
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
	}
	public static Map<String, Map<String,String>> getData(){
		System.out.println("getData");
		List<String> itermIds = getItermIdFromMysql();
		Map<String,Map<String,String>> map = new HashMap<>();
		for (int i = 0;i < 100;i++){
			Map<String,String> info = new HashMap<>();
			while (info.size() < 100){
				int index = random.nextInt(1000);
				int score = random.nextInt(10) + 1;
				info.put(itermIds.get(index),String.valueOf(score));
			}
			map.put(String.valueOf(i + 1),info);
		}
		System.out.println("end");
		return map;
	}
	public static List<String> getItermIdFromMysql(){
		System.out.println("getItermIdFromMysql");
		List<String> list = new LinkedList<>();
		String sql = "select itermId from iterm";
		try{
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()){
				list.add(resultSet.getString("itermId"));
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
		System.out.println("end");
		return list;
	}
	public static void main(String[] args){
		System.out.println("开始");
		dataToMysql();
		System.out.println("end");
	}
}
