/**
 * ClassName:DataBaseHandle.java
 * Date:2018年6月4日
 */
package com.idata.data;

import java.sql.ResultSet;
import java.sql.Statement;

import com.ojdbc.sql.ConnectionManager;
import com.ojdbc.sql.ConnectionManager.ConnectionInfo;
import com.ojdbc.sql.ConnectionObject;
import com.ojdbc.sql.DataBase;
import com.ojdbc.sql.DataBaseEnum;
import com.ojdbc.sql.DataBaseManager;

/**
 * Creater:SHAO Gaige
 * Description:数据库处理类
 * Log:
 */
public class DataBaseHandle {
	
	public DataBase DB;
	
	private DataBaseEnum d_enum = null;
	
	private String dataBaseurl;
	
	private String userName;
	
	private String passWord;
	
	public DataBaseHandle(String url,String username,String password) 
	{
		dataBaseurl = url;
		userName = username;
		passWord = password;
		init();
	}
	
	private boolean init()
	{
		if(dataBaseurl.contains("oracle"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.ORACLE, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.ORACLE;
		}
		else if(dataBaseurl.contains("sqlserver"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.SQLSERVER, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.SQLSERVER;
		}
		else if(dataBaseurl.contains("mysql"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.MYSQL, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.MYSQL;
		}
		else if(dataBaseurl.contains("postgresql"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.POSTGRESQL, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.POSTGRESQL;
		}
		 
		if(DB != null)
		{
			ConnectionObject conn = ConnectionManager.borrowConnectionObject(d_enum, dataBaseurl, userName, passWord);
			if(conn != null)
			{
				ConnectionManager.returnConnectionObject(conn);
				return true;
			}
			else
			{
				return false;
			}
			
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * 获取ConnectionInfo
	 * @return ConnectionInfo
	 */
	public ConnectionInfo getConnectionInfo()
	{
		return ConnectionManager.getConnectionInfo(d_enum, dataBaseurl, userName, passWord);
	}
	
	/**
	 * 判断表是否存在
	 * @param tableName
	 * @return
	 */
	public boolean isTableExist(String tableName)
	{
		String sql = "select count(*) from "+tableName;
		ConnectionObject conn = null;
		try 
		{
			conn = ConnectionManager.borrowConnectionObject(getConnectionInfo());
			Statement stat = conn.getConnection().createStatement();
			ResultSet rs = stat.executeQuery(sql);
			if(rs.next())
			{
				rs.close();
				stat.close();
				return true;
			}
			else
			{
				rs.close();
				stat.close();
				return false;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			return false;
		}finally{
			if(conn != null)
			{
				ConnectionManager.returnConnectionObject(conn);
			}
		}
	}
}
