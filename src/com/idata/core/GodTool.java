/**
 * ClassName: GodTool.java
 * Date: 2017年6月10日
 */
package com.idata.core;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

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
	/**
	 * 利用反射构建对象
	 * @param connInfo
	 * @return DataBase
	 */
	public static Object newInstance(String className)
	{
		try 
		{
			Object obj = null;
			obj = Class.forName(className).newInstance();
	        return obj;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 利用反射构建对象
	 * @param connInfo
	 * @return DataBase
	 */
	public static Object newInstance(Class<?> d)
	{
		try 
		{
	        return d.newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * java反射调用函数方法
	 * @param className
	 * @param funcationName
	 * @param params
	 * @return
	 */
	public static String invoke(String className,String funcationName,String params)
	{
		Object rs = null;
		try 
		{
			@SuppressWarnings("rawtypes")
			Class cs = Class.forName(className);
			@SuppressWarnings("unchecked")
			Method method = cs.getDeclaredMethod(funcationName, String.class);
			if (Modifier.isStatic(method.getModifiers())) 
			{
				//static
				rs = method.invoke(cs, params);
			}
			else
			{
				//no static
				rs = method.invoke(cs.newInstance(),params);
			}
			return rs.toString();
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
