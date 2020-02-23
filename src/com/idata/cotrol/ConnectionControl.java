/**
 * ClassName:ConnectionControl.java
 * Date:2020年1月29日
 */
package com.idata.cotrol;

import com.idata.core.OneDataServer;
import com.idata.tool.AESUtil;

/**
 * Creater:SHAO Gaige
 * Description:数据库连接串控制器
 * Log:
 */
public class ConnectionControl {
	
	
	public String getDecodeConStr(String data)
	{
		return AESUtil.aesDecrypt(data, OneDataServer.AESKEY);
	}
	
	public String getEncodeConStr(String data)
	{
		String[] arr = data.split(",");
		if("postgresql".equalsIgnoreCase(arr[0]) || arr[0].contains("postgresql")
				|| "postgis".equalsIgnoreCase(arr[0]) || arr[0].contains("postgis"))
		{
			String constr = "jdbc:postgresql://"+arr[1]+","+arr[2]+","+arr[3];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		else if("oracle".equalsIgnoreCase(arr[0]) || arr[0].contains("oracle"))
		{
			String constr = "jdbc:oracle:thin:@"+arr[1]+","+arr[2]+","+arr[3];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		else if("mysql".equalsIgnoreCase(arr[0]) || arr[0].contains("mysql"))
		{
			String constr = "jdbc:mysql://"+arr[1]+","+arr[2]+","+arr[3];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		else if("sqlserver".equalsIgnoreCase(arr[0]) || arr[0].contains("sqlserver"))
		{
			//jdbc:sqlserver://127.0.0.1:1433;DatabaseName=test
			String constr = "sqlserver://"+arr[1]+","+arr[2]+","+arr[3];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		else if("mongo".equalsIgnoreCase(arr[0]) || arr[0].contains("mongo"))
		{
			//jdbc:mongo://127.0.0.1:29847/test
			String constr = "jdbc:mongo://"+arr[1]+","+arr[2]+","+arr[3];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		else if("sqlite".equalsIgnoreCase(arr[0]) || arr[0].contains("sqlite"))
		{
			//jdbc:sqlite://d:/test.db
			String constr = "jdbc:sqlite://"+arr[1];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		else if("mdb".equalsIgnoreCase(arr[0]) || arr[0].contains("mdb"))
		{
			String constr = arr[1];
			return AESUtil.aesEncrypt(constr, OneDataServer.AESKEY);
		}
		
		return data;
	}

}
