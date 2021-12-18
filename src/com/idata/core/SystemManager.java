/**
 * ClassName:SystemManager.java
 * Date:2021年3月12日
 */
package com.idata.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;
import com.idata.tool.RandomIDUtil;
import com.idata.tool.RedisUtil;
import com.ojdbc.sql.DataBaseEnum;

/**
 * Creater:SHAO Gaige
 * Description:系统管理类
 * Log:
 */
public class SystemManager {
	
	/**
	 * 初始化redis
	 */
	public static void initRedis()
	{
		OneDataServer.redisUtil = RedisUtil.getRedisUtil();
		LogUtil.info("Redis配置初始化成功...");
	}

	/**
	 * 初始化Hbase
	 */
	public static void initHbase()
	{
		//初始化HBase
		Configuration configuration = null;
		configuration = HBaseConfiguration.create();
        //必须配置1
        configuration.set("hbase.zookeeper.quorum", 
        		PropertiesUtil.getValue("hbase.zookeeper.quorum"));
        //必须配置2
        configuration.set("hbase.rootdir", "/");
        configuration.set("hbase.cluster.distributed", "true");
        configuration.set("zookeeper.session.timeout", 
        		PropertiesUtil.getValue("zookeeper.session.timeout"));
        configuration.set("hbase.hregion.majorcompaction", "0");
        configuration.set("hbase.regionserver.regionSplitLimit", "1");
        configuration.set("dfs.client.socket-timeout", 
        		PropertiesUtil.getValue("dfs.client.socket-timeout"));
        configuration.set("hbase.regionserver.handler.count", 
        		PropertiesUtil.getValue("hbase.regionserver.handler.count"));
		configuration.set("hbase.zookeeper.property.clientPort",
				PropertiesUtil.getValue("hbase.zookeeper.property.clientPort"));
		OneDataServer.hbaseHandle = new HbaseHandle(configuration);
		LogUtil.info("HBase配置初始化成功...");
	}
	
	/**
	 * 获取索引的路径
	 * @param table
	 * @return
	 */
	public static String getTableIndexPath(String table)
	{
		String indexpath = PropertiesUtil.getValue("TABLES_INDEX_PATH");
		if(!indexpath.endsWith("/"))
		{
			indexpath += "/";
		}
		Calendar now = Calendar.getInstance();
		int year = now.get(Calendar.YEAR);
		int month = now.get(Calendar.MONTH) + 1;
		indexpath += year+"/"+month+"/";
		String path = indexpath+table;
		File file = new File(path);
		if(file.exists())
		{
			path = indexpath+table+RandomIDUtil.getDate("_");
			file = new File(path);
		}
		file.mkdirs();
		return path;
	}
	
	/**
	 * 获取索引的读取句柄
	 * @param path
	 * @return
	 */
	public static IndexSearcher getTableSeracher(String path)
	{
		if(OneDataServer.tableReader.containsKey(path))
		{
			return  new IndexSearcher(OneDataServer.tableReader.get(path));
		}
		else
		{
			try 
			{
				Directory dir = FSDirectory.open(Paths.get(path));
				if(DirectoryReader.indexExists(dir))
				{
					IndexReader reader = DirectoryReader.open(dir);
					IndexSearcher searcher = new IndexSearcher(reader);
					OneDataServer.tableReader.put(path, reader);
					OneDataServer.tableOrder.offer(path);
					return searcher;
				}
				else
				{
					return null;
				}
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				LogUtil.error("读取索引出现错误："+e.getMessage());
				return null;
			}
		}
	}
	
	/**
	 * 初始化表格，判断表是否存在，不存在就创建
	 * @return
	 */
	public static boolean initTables()
	{
		DataBaseHandle SQLITEDBHandle = OneDataServer.SQLITEDBHandle;
		//sqlite
		//onedata_datavisit
		if(!SQLITEDBHandle.isTableExist(OneDataServer.TablePrefix+"datavisit"))
		{
			String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"datavisit");
			SQLITEDBHandle.exeSQLCreate(sql);
		}
		//onedata_visit
		if(!SQLITEDBHandle.isTableExist(OneDataServer.TablePrefix+"visit"))
		{
			String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"visit");
			SQLITEDBHandle.exeSQLCreate(sql);
		}
		
		DataBaseHandle SystemDBHandle = OneDataServer.SystemDBHandle;
		if(SystemDBHandle.getDataBaseType()==DataBaseEnum.SQSLITE)
		{
			//sqlite
			//onedata_userinfo
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"userinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"userinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"databaseinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"databaseinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"tableinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"tableinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"fieldsinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"fieldsinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"applicationinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"applicationinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"interfaceinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"interfaceinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"functioninfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"functioninfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"mapinfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"mapinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"token"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"token");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"warninfo"))
			{
				String sql = PropertiesUtil.getValue("sqlite_"+OneDataServer.TablePrefix+"warninfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			
		}
		else if(SystemDBHandle.getDataBaseType()==DataBaseEnum.POSTGRESQL)
		{
			//postgresql
			//onedata_userinfo
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"userinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"userinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"databaseinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"databaseinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"tableinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"tableinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"fieldsinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"fieldsinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"applicationinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"applicationinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"interfaceinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"interfaceinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"functioninfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"functioninfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"mapinfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"mapinfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"token"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"token");
				SystemDBHandle.exeSQLCreate(sql);
			}
			if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"warninfo"))
			{
				String sql = PropertiesUtil.getValue("postgres_"+OneDataServer.TablePrefix+"warninfo");
				SystemDBHandle.exeSQLCreate(sql);
			}
			
		}
		else if(OneDataServer.SystemDBHandle.getDataBaseType()==DataBaseEnum.MYSQL)
		{
			
		}
		return true;
	}
	
	public static void stopServer()
	{
		setSystemHandle(null);
		OneDataServer.databaseinfo = null;
		OneDataServer.resultcache = null;
	}
	
	public static boolean reload()
	{
		OneDataServer.init();
		return true;
	}
	
	public static void setSystemHandle(DataBaseHandle handle)
	{
		OneDataServer.SystemDBHandle = handle;
	}
	
	public static boolean isJSON(String str) {
	    boolean result = false;
	    try 
	    {
	    	JsonObject contents = new JsonParser().parse(str).getAsJsonObject();
	    	if(contents != null)
	    	{
	    		result = true;
	    	}
	    } 
	    catch (Exception e) 
	    {
	        result = false;
	    }
	    return result;
	}

}
