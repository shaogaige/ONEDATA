/**
 * ClassName:DataBaseHandle.java
 * Date:2018年6月4日
 */
package com.idata.core;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.idata.tool.AESUtil;
import com.ojdbc.sql.ConnectionManager;
import com.ojdbc.sql.ConnectionManager.ConnectionInfo;
import com.ojdbc.sql.core.SQLPreparedParamUtil;
import com.ojdbc.sql.exception.DBCException;
import com.ojdbc.sql.ConnectionObject;
import com.ojdbc.sql.DataBase;
import com.ojdbc.sql.DataBaseEnum;
import com.ojdbc.sql.DataBaseManager;
import com.ojdbc.sql.PreparedParam;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:数据库处理类
 * Log:
 */
public class DataBaseHandle extends DataBase {
	
	private DataBaseEnum d_enum = null;
	
	private String dataBaseurl;
	
	private String userName;
	
	private String passWord;
	
	private boolean available = false;
	
	public DataBaseHandle(String encrypt) throws Exception 
	{
		String content = AESUtil.aesDecrypt(encrypt, OneDataServer.AESKEY);
		String[] ds = content.split(",");
		if(ds.length == 3)
		{
			dataBaseurl = ds[0];
			userName = ds[1];
			passWord = ds[2];
			if(!access())
			{
				throw new Exception("数据库连接串不可用");
			}
		}
		else if(ds.length == 1)
		{
			dataBaseurl = ds[0];
			if(!access())
			{
				throw new Exception("数据库连接串不可用");
			}
		}
	}
	
	public DataBaseHandle(String url,String username,String password) throws Exception 
	{
		dataBaseurl = url;
		userName = username;
		passWord = password;
		if(!access())
		{
			throw new Exception("数据库连接串不可用");
		}
	}
	
	private boolean access()
	{
		DataBase DB = null;
		if(dataBaseurl.contains("postgresql"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.POSTGRESQL, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.POSTGRESQL;
		}
		else if(dataBaseurl.contains("oracle"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.ORACLE, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.ORACLE;
		}
		else if(dataBaseurl.contains("mysql"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.MYSQL, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.MYSQL;
		}
		else if(dataBaseurl.contains("sqlserver"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.SQLSERVER, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.SQLSERVER;
		}
		else if(dataBaseurl.contains("mongo"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.MONGODB, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.MONGODB;
		}
		else if(dataBaseurl.contains("sqlite"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.SQSLITE, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.SQSLITE;
		}
		else if(dataBaseurl.contains("mdb"))
		{
			DB = DataBaseManager.getDataBase(DataBaseEnum.ACCESS, dataBaseurl, userName,passWord);
			d_enum = DataBaseEnum.ACCESS;
		}	
		
		if(DB != null)
		{
			super.connInfo = ConnectionManager.getConnectionInfo(d_enum, dataBaseurl, userName, passWord);
			ConnectionObject conn = ConnectionManager.borrowConnectionObject(d_enum, dataBaseurl, userName, passWord);
			if(conn != null)
			{
				ConnectionManager.returnConnectionObject(conn);
				available = true;
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
	
	public boolean isAvailable() {
		return available;
	}
	
	public List<SuperObject> exeSQLSelect2(String sql, int start, int count,String idfield,String geofield) {
		// TODO Auto-generated method stub
		ConnectionObject conn = null;
		Statement stat = null;
		ResultSet rs = null;
		try
		{
			conn = ConnectionManager.borrowConnectionObject(connInfo);
			stat = conn.getConnection().createStatement();
			rs = stat.executeQuery(sql);
			List<SuperObject> r = ResultBuilder.getResultSet(rs,start,count,idfield,geofield);
			return r;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			DBCException.logException(DBCException.E_SQL, e);
			return null;
		}
		finally
		{
			try 
			{
				if(rs != null)
				{
					rs.close();
				}
				if(stat != null)
				{
					stat.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(conn != null)
			{
				ConnectionManager.returnConnectionObject(conn);
			}
		}
	}
	
	public List<SuperObject> exePreparedSQLSelect2(String sql, PreparedParam preparedParam, int start, int count,String idfield,String geofield) {
		// TODO Auto-generated method stub
		ConnectionObject conn = null;
		PreparedStatement preStmt = null;
		ResultSet rs = null;
		try 
		{
			conn = ConnectionManager.borrowConnectionObject(connInfo);
			preStmt = conn.getConnection().prepareStatement(sql);
			SQLPreparedParamUtil.setSQLPreparedParam(preStmt, preparedParam);
			rs = preStmt.executeQuery();
			List<SuperObject> r = ResultBuilder.getResultSet(rs,start,count,idfield,geofield);
			return r;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			DBCException.logException(DBCException.E_SQL, e);
			return null;
		}
		finally
		{
			try 
			{
				if(rs != null)
				{
					rs.close();
				}
				if(preStmt != null)
				{
					preStmt.close();
				}
			}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(conn != null)
			{
				ConnectionManager.returnConnectionObject(conn);
			}
		}
	}

	public List<SuperObject> getMetaData2(String sql) {
		// TODO Auto-generated method stub
		ConnectionObject conn = null;
		Statement stat = null;
		ResultSet rs = null;
		try 
		{
			conn = ConnectionManager.borrowConnectionObject(connInfo);
			stat = conn.getConnection().createStatement();
			rs = stat.executeQuery(sql);
			ResultSetMetaData resultMetaData = rs.getMetaData();
			List<SuperObject> os = new ArrayList<SuperObject>();
			int size = resultMetaData.getColumnCount();
			//获取所有列名称
			for(int i=1;i<=size;i++)
			{
				SuperObject o = new SuperObject();
				o.addProperty("name", new Value().setString_value(resultMetaData.getColumnName(i)));
				o.addProperty("label", new Value().setString_value(resultMetaData.getColumnLabel(i)));
				o.addProperty("typename", new Value().setString_value(resultMetaData.getColumnTypeName(i)));
				o.addProperty("type", new Value().setInt_value(resultMetaData.getColumnType(i)));
				o.addProperty("scale", new Value().setInt_value(resultMetaData.getScale(i)));
				o.addProperty("precision", new Value().setInt_value(resultMetaData.getPrecision(i)));
				os.add(o);
			}
			
			return os;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			DBCException.logException(DBCException.E_SQL, e);
			return null;
		}
		finally
		{
			try 
			{
				if(rs != null)
				{
					rs.close();
				}
				if(stat != null)
				{
					stat.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
			if(conn != null)
			{
				ConnectionManager.returnConnectionObject(conn);
			}
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
