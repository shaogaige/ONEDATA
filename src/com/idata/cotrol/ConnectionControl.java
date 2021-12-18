/**
 * ClassName:ConnectionControl.java
 * Date:2020年1月29日
 */
package com.idata.cotrol;

import java.util.List;

import com.idata.core.DataBaseHandle;
import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.core.SystemManager;
import com.idata.data.DataBaseDriver;
import com.idata.tool.AESUtil;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:数据库连接串控制器
 * Log:
 */
public class ConnectionControl extends DataControl{
	
	public String process(DataParam param)
	{
		String result = "";
		if("query".equalsIgnoreCase(param.getOperation()))
		{
		    result = query(param);
		}
		else if("check".equalsIgnoreCase(param.getOperation()))
		{
			result = check(param);
		}
		else if("add".equalsIgnoreCase(param.getOperation()))
		{
			result = add(param);
		}
		else if("edit".equalsIgnoreCase(param.getOperation()))
		{
			result = edit(param);
		}
		else if("delete".equalsIgnoreCase(param.getOperation()))
		{
			result = delete(param);
		}
		else if("encode".equalsIgnoreCase(param.getOperation()))
		{
		    result = add(param);
		}
		else if("decode".equalsIgnoreCase(param.getOperation()))
		{
			result = decode(param.getJsondata());
		}
		
		return result;
	}
	
	public String check(DataParam dataParam)
	{
		String encodeStr = encode(dataParam,null);
		try 
		{
			DataBaseHandle d = new DataBaseHandle(encodeStr);
			boolean f = d.isTableExist(dataParam.getType());
			if(f)
			{
				return "{\"state\":true,\"message\":\"数据库连接串及表名验证通过\",\"size\":0,\"data\":[]}";
			}
			else
			{
				return "{\"state\":false,\"message\":\"数据库不存在该表\",\"size\":0,\"data\":[]}";
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return "{\"state\":false,\"message\":\"数据库连接串不可用\",\"size\":0,\"data\":[]}";
		}
	}
	
	public String edit(DataParam dataParam)
	{
		dataParam.setJsonDataProperty("update_time", new Value().setString_value(OneDataServer.getCurrentTime()));
		//重新生成相关串
		encode(dataParam,null);
		dataParam.setJsonDataProperty("status", new Value().setString_value("true"));
		
		OneDataServer.databaseinfo.remove(String.valueOf(dataParam.getJsonDataValue("id").getInt_value()));
		
		DataControl dataCon = new DataControl();
		return dataCon.process(dataParam);
	}
	
	public String query(DataParam dataParam)
	{
		if(dataParam.getQueryfields() == null || "".equalsIgnoreCase(dataParam.getQueryfields()))
		{
			dataParam.setQueryfields("status");
			dataParam.setQueryvalues("true");
		}
		else
		{
			dataParam.setQueryfields(dataParam.getQueryfields()+",status");
			dataParam.setQueryvalues(dataParam.getQueryvalues()+",true");
		}
		
		DataControl dataCon = new DataControl();
		return dataCon.process(dataParam);
	}
	
	public String delete(DataParam dataParam)
	{
		dataParam.setOperation("edit");
		dataParam.setJsonDataProperty("status", new Value().setString_value("false"));
		dataParam.setJsonDataProperty("update_time", new Value().setString_value(OneDataServer.getCurrentTime()));
		
		OneDataServer.databaseinfo.remove(String.valueOf(dataParam.getJsonDataValue("id").getInt_value()));
		
		DataControl dataCon = new DataControl();
		return dataCon.process(dataParam);
	}
	
	public String add(DataParam dataParam)
	{
		String data = encode(dataParam,null);
		//记录数据库信息
		//DataParam dataParam = new DataParam();
		//dataParam.setCon(OneDataServer.SystemDBHandle.getEncodeConStr());
		//dataParam.setLayer(OneDataServer.TablePrefix+"databaseinfo");
		//dataParam.setOperation("add");
		//dataParam.setJsondata("{\"database_type\":\""+arr[0]+"\",\"database_conname\":\""+data+"\",\"server_ip\":\""+arr[1]+"\",\"server_user\":\"***\",\"server_password\":\"***\",\"database_url\":\""+constr+"\",\"register_time\":\""+OneDataServer.getCurrentTime()+"\",\"status\":true}");
		dataParam.setQueryfields("conencode");
		dataParam.setQueryoperates("=");
		dataParam.setQueryvalues(data);
		DataBaseDriver databaseDriver = new DataBaseDriver();
		databaseDriver.setDataBaseHandle(OneDataServer.SystemDBHandle);
		//先去查询
		List<SuperObject> rs = databaseDriver.query(dataParam);
		if(rs == null || rs.size()<1)
		{
			databaseDriver.add(dataParam);
			rs = databaseDriver.query(dataParam);
		}
		
		if(rs != null && rs.size() >0)
		{
			int id = rs.get(0).getProperty("id").getInt_value();
			data = "{\"state\":true,\"message\":\"数据处理成功\",\"uid\":\""+id+"\",\"con\":\""+data+"\"}";
			OneDataServer.databaseinfo.add(String.valueOf(id),rs.get(0));
		}
		else
		{
			data = "{\"state\":false,\"message\":\"数据处理失败\",\"uid\":\"\",\"con\":\"\"}";
		}
		
		return data;		
	}
	
	
	public String decode(String data)
	{
		return AESUtil.aesDecrypt(data, OneDataServer.AESKEY);
	}
	
	public String encode(DataParam dataParam,String data)
	{
		String conencode = "";
		if(dataParam == null)
		{
			dataParam = new DataParam();
		}
		if(data == null || "".equalsIgnoreCase(data))
		{
			data = dataParam.getJsondata();
		}
		if(SystemManager.isJSON(data))
		{
			//设置默认的参数值
			if(dataParam.getJsonDataValue("register_time") == null || "".equalsIgnoreCase(dataParam.getJsonDataValue("register_time").getString_value()))
			{
				dataParam.setJsonDataProperty("register_time", new Value().setString_value(OneDataServer.getCurrentTime()));
			}
			dataParam.setJsonDataProperty("update_time", new Value().setString_value(OneDataServer.getCurrentTime()));
			//database_url
			String database_url = "";
			String server_ip = dataParam.getJsonDataValue("server_ip").getString_value();
			String server_port = dataParam.getJsonDataValue("server_port").getString_value();
			String database_name = dataParam.getJsonDataValue("database_name").getString_value();
			String server_user = dataParam.getJsonDataValue("server_user").getString_value();
			String server_password = dataParam.getJsonDataValue("server_password").getString_value();
			
			if("postgresql".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = "jdbc:postgresql://"+server_ip+":"+server_port+"/"+database_name+","+server_user+","+server_password;
			}
			else if("oracle".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = "jdbc:oracle:thin:@"+server_ip+":"+server_port+":"+database_name+","+server_user+","+server_password;
			}
			else if("mysql".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = "jdbc:mysql://"+server_ip+":"+server_port+"/"+database_name+","+server_user+","+server_password;
			}
			else if("sqlserver".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = "jdbc:sqlserver://"+server_ip+":"+server_port+";DatabaseName="+database_name+","+server_user+","+server_password;
			}
			else if("mongo".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = "jdbc:mongo://"+server_ip+":"+server_port+"/"+database_name+","+server_user+","+server_password;
			}
			else if("sqlite".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = "jdbc:sqlite://"+server_ip;
			}
			else if("mdb".equalsIgnoreCase(dataParam.getJsonDataValue("database_type").getString_value()))
			{
				database_url = server_ip;
			}
			dataParam.setJsonDataProperty("database_url", new Value().setString_value(database_url));
			//conencode
			conencode = AESUtil.aesEncrypt(database_url, OneDataServer.AESKEY);
			dataParam.setJsonDataProperty("conencode", new Value().setString_value(conencode));
			dataParam.setJsonDataProperty("status", new Value().setString_value("true"));
		}
		else
		{
			String[] arr = data.split(",");
			
			String constr = "***";
			String ip = "***";
			String port = "***";
			String databasename = "***";
			String user = "***";
			String password = "***";
		
			if("postgresql".equalsIgnoreCase(arr[0]) || arr[0].contains("postgresql")
					|| "postgis".equalsIgnoreCase(arr[0]) || arr[0].contains("postgis"))
			{
				//jdbc:postgresql://127.0.0.1:5432/postgis
				constr = "jdbc:postgresql://"+arr[1]+","+arr[2]+","+arr[3];
				ip = arr[1].substring(0, arr[1].lastIndexOf(":"));
				port = arr[1].substring(arr[1].lastIndexOf(":")+1, arr[1].lastIndexOf("/"));
				databasename = arr[1].substring(arr[1].lastIndexOf("/")+1, arr[1].length());
				user = arr[2];
				password = arr[3];
			}
			else if("oracle".equalsIgnoreCase(arr[0]) || arr[0].contains("oracle"))
			{
				//jdbc:oracle:thin:@127.0.0.1:1521:orcl
				constr = "jdbc:oracle:thin:@"+arr[1]+","+arr[2]+","+arr[3];
				databasename = arr[1].substring(arr[1].lastIndexOf(":")+1, arr[1].length());
				String temp = arr[1].substring(0,arr[1].lastIndexOf(":"));
				ip = arr[1].substring(0, arr[1].lastIndexOf(":"));
				port = temp.substring(temp.lastIndexOf(":")+1, temp.length());
				user = arr[2];
				password = arr[3];
			}
			else if("mysql".equalsIgnoreCase(arr[0]) || arr[0].contains("mysql"))
			{
				//jdbc:mysql://127.0.0.1:3306/test
				constr = "jdbc:mysql://"+arr[1]+","+arr[2]+","+arr[3];
				ip = arr[1].substring(0, arr[1].lastIndexOf(":"));
				port = arr[1].substring(arr[1].lastIndexOf(":")+1, arr[1].lastIndexOf("/"));
				databasename = arr[1].substring(arr[1].lastIndexOf("/")+1, arr[1].length());
				user = arr[2];
				password = arr[3];
			}
			else if("sqlserver".equalsIgnoreCase(arr[0]) || arr[0].contains("sqlserver"))
			{
				//jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test
				constr = "jdbc:sqlserver://"+arr[1]+","+arr[2]+","+arr[3];
				ip = arr[1].substring(0, arr[1].lastIndexOf(":"));
				port = arr[1].substring(arr[1].lastIndexOf(":")+1, arr[1].lastIndexOf(";"));
				databasename = arr[1].substring(arr[1].lastIndexOf("=")+1, arr[1].length());
				user = arr[2];
				password = arr[3];
			}
			else if("mongo".equalsIgnoreCase(arr[0]) || arr[0].contains("mongo"))
			{
				//jdbc:mongo://127.0.0.1:29847/test
				constr = "jdbc:mongo://"+arr[1]+","+arr[2]+","+arr[3];
				ip = arr[1].substring(0, arr[1].lastIndexOf(":"));
				port = arr[1].substring(arr[1].lastIndexOf(":")+1, arr[1].lastIndexOf("/"));
				databasename = arr[1].substring(arr[1].lastIndexOf("/")+1, arr[1].length());
				user = arr[2];
				password = arr[3];
			}
			else if("sqlite".equalsIgnoreCase(arr[0]) || arr[0].contains("sqlite"))
			{
				//jdbc:sqlite://d:/test.db
				constr = "jdbc:sqlite://"+arr[1];
				ip = arr[1];
			}
			else if("mdb".equalsIgnoreCase(arr[0]) || arr[0].contains("mdb"))
			{
				//e://student.mdb
				constr = arr[1];
				ip = arr[1];
			}
			
			conencode = AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
			dataParam.setJsondata("{\"database_type\":\""+arr[0]+"\",\"database_conname\":\""+conencode+"\",\"conencode\":\""+conencode
					+"\",\"server_ip\":\""+ip+"\",\"server_port\":\""+port+"\",\"server_user\":\""+user
					+"\",\"server_password\":\""+password+"\",\"database_name\":\""+databasename+"\",\"database_url\":\""
					+constr+"\",\"register_time\":\""+OneDataServer.getCurrentTime()+"\",\"status\":\"true\"}");
		}
		
		return conencode;
		
	}

}
