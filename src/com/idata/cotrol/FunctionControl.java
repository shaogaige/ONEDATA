/**
 * ClassName:FunctionControl.java
 * Date:2021年9月23日
 */
package com.idata.cotrol;

import com.idata.core.GodTool;

/**
 * Creater:SHAO Gaige
 * Description:类函数访问管理器
 * Log:
 */
public class FunctionControl {
	
	public String process(String className,String funName,String params)
	{
		//启动上帝调用函数模式
		String rs = GodTool.invoke(className, funName, params);
		if(rs == null)
		{
			rs = "{\"state\":false,\"message\":\"内部出现错误，请联系后台管理员！\",\"size\":0,\"data\":[]}";
		}
		else
		{
			if(rs.startsWith("[") && rs.endsWith("]"))
			{
				rs = "{\"state\":true,\"message\":\"函数调用成功！\",\"size\":1,\"data\":"+rs+"}";
			}
			else if(rs.startsWith("{") && rs.endsWith("}")) 
			{
				rs = "{\"state\":true,\"message\":\"函数调用成功！\",\"size\":1,\"data\":["+rs+"]}";
			}
			else
			{
				rs = "{\"state\":true,\"message\":\"函数调用成功！\",\"size\":1,\"data\":\""+rs+"\"}";
			}
			//rs = "{\"state\":true,\"message\":\"函数调用成功！\",\"size\":"+0+",\"data\":"+rs+"}";
		}
		return rs;
	}

}
