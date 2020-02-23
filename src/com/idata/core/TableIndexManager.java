/**
 * ClassName:TableIndexManager.java
 * Date:2019年4月2日
 */
package com.idata.core;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.idata.tool.FileUtil;
import com.idata.tool.PropertiesUtil;

/**
 * Creater:SHAO Gaige
 * Description:数据表索引管理
 * Log:
 */
public class TableIndexManager {
	
	/**
	 * 创建索引
	 * @param constring
	 * @param tableName
	 */
	public static void createTableIndex(String constring,String tableName,String geoField,String path)
	{
		Thread thread = new Thread(new TableIndexWriter(constring,tableName,geoField,path));
		thread.start();
	}
	
	/**
	 * 删除索引
	 * @param tableName
	 */
	public static void deleteTableIndex(String tableName)
	{
		try 
		{
			IndexReader reader = OneDataServer.tableReader.remove(tableName);
			reader.close();
			reader = null;
			
			OneDataServer.tableOrder.remove(tableName);
			
			File file = new File(PropertiesUtil.getValue("TABLES_INDEX_PATH")+"/"+tableName);
			FileUtil.delFile(file);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
