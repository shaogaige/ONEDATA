/**
 * ClassName:HbaseManager.java
 * Date:2020年3月2日
 */
package com.idata.core;

/**
 * Creater:SHAO Gaige
 * Description:Hbase创建表、导入表和删除表管理类
 * Log:
 */
public class HbaseManager {
	
	
	public static boolean createTable(String tableName,String[] fields)
	{
		return OneDataServer.hbaseHandle.createTable(tableName, fields); 
	}
	
	public static void importData(String constring,String tableName,String idField,String geoField,String newTable,boolean createAndAppend)
	{
		HbaseImporter p = new HbaseImporter(constring,tableName,idField,geoField,newTable,createAndAppend);
		p.write();
	}
	public static void importDataByThread(String constring,String tableName,String idField,String geoField,String newTable,boolean createAndAppend)
	{
		Thread thread = new Thread(new HbaseImporter(constring,tableName,idField,geoField,newTable,createAndAppend));
		thread.start();
	}
	
	public static boolean deleteTable(String tableName)
	{
		return OneDataServer.hbaseHandle.deleteTable(tableName);
	}
	
}
