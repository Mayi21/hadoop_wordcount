package DataBase;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySqlServerDao {
	public static Connection AD(int a){
		Connection connection = getConnection();
		if (a == 0){
			releaseConnection(connection);
		}
		return connection;
	}
	public static Connection getConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String connectionURL = "jdbc:mysql://localhost:3306/jingdong?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false";
			Connection connect = DriverManager.getConnection(connectionURL,"root","123456");
			return connect;
		}catch (Exception e) {
			System.out.print("get data error!");
			e.printStackTrace();
		}
		return null;
	}
	public static void releaseConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("shutdown!");
	}
}
