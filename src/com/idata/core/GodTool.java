/**
 * ClassName: GodTool.java
 * Date: 2017年6月10日
 */
package com.idata.core;

import com.idata.data.IDataDriver;

/**
 * Author: ShaoGaige
 * Description: GodTool反射类
 * Log: 
 */
public class GodTool {
	/**
	 * 利用反射构建DataBase对象
	 * @param connInfo
	 * @return DataBase
	 */
	public static IDataDriver newInstanceDataDriver(Class<?> d)
	{
		try 
		{
			IDataDriver dataDriver= (IDataDriver)d.newInstance(); 
			return dataDriver;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
