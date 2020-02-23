/**
 * ClassName:OneDataServer.java 
 * Date:2020年1月9日
 */
package com.idata.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.idata.cotrol.VisitControl.VisitLogModel;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;
import com.idata.tool.RandomIDUtil;
/**
 * Creater:SHAO Gaige
 * Description:服务初始化类
 * Log:
 */
public class OneDataServer {
	
	//数据库工具data&share
    public static DataBaseHandle SystemDBHandle = null;
    //token&visit
    public static DataBaseHandle SQLITEDBHandle = null;
	//数据库表前缀
	public static String TablePrefix = "";
	//序列前缀
	public static String SEQPrefix = "";
	//服务节点
	public static String CurrentServerNode = "localhost:8090/ONEDATA";
	//索引读写
	public static Map<String,IndexReader> tableReader = new ConcurrentHashMap<String,IndexReader>();
	public static Queue<String> tableOrder = new ConcurrentLinkedQueue<String>();
	//备份节点
    public static boolean BackUp = false;
	public static String ServerBackUp;
	public static String FilePathBackUp;
	//访问日志
	public static boolean visitlog = true;
	//异常日志路径
	public static String logPath;
	//日期格式化
	public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	//访问次数缓存
	public static Map<String,Long> visitCache = new ConcurrentHashMap<String,Long>();
	public static ConcurrentLinkedQueue<VisitLogModel> visitorqueue = new ConcurrentLinkedQueue<VisitLogModel>();
	//内部字符串AES加密秘钥
	public final static String AESKEY = "ONEDATASTRENCKEY";
	
	//系统初始化
	public static void init()
	{
		System.out.println("ONEDATA平台正在启动中...");
		//连接数据库
		PropertiesUtil.init();
		TablePrefix = PropertiesUtil.getValue("TABLE_PREFIX");
		SEQPrefix = PropertiesUtil.getValue("SEQ_PREFIX");
		String url = PropertiesUtil.getValue("DataBaseURL");
		String name = PropertiesUtil.getValue("UserName");
		String password = PropertiesUtil.getValue("Password");
		String sqlitepath = PropertiesUtil.getValue("SQLITEPATH");
		try 
		{
			SystemDBHandle = new DataBaseHandle(url,name,password);
			String sqliteurl = "jdbc:sqlite://"+sqlitepath;
			SQLITEDBHandle = new DataBaseHandle(sqliteurl,null,null);
			//初始化表
			initTbales();
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("配置的数据库连接不可用");
		}
		//初始化服务节点
		initServerNode();
		//初始化索引路径
		String indexPath = PropertiesUtil.getValue("SERVER_INDEXPATH");
		System.out.println("INDEX Path: "+indexPath);
		//异常日志
		logPath = PropertiesUtil.getValue("LOGFILE_PATH");
		System.out.println("异常日志文件路径:"+logPath);
		File logfile = new File(logPath + "/log.txt");
		LogUtil.setLogFile(logfile);
		//访问日志
		String vlog = PropertiesUtil.getValue("VISIT_LOG");
		if("true".equalsIgnoreCase(vlog))
		{
			visitlog = true;
			System.out.println("访问日志开启...");
		}
		else
		{
			System.out.println("访问日志未开启!");
		}
		
//		//初始化HBase
//		Configuration configuration = null;
//		configuration = HBaseConfiguration.create();
//        //必须配置1
//        configuration.set("hbase.zookeeper.quorum", 
//        		PropertiesUtil.getValue("hbase.zookeeper.quorum"));
//        //必须配置2
//        configuration.set("hbase.rootdir", "/");
//        configuration.set("hbase.cluster.distributed", "true");
//        configuration.set("zookeeper.session.timeout", 
//        		PropertiesUtil.getValue("zookeeper.session.timeout"));
//        configuration.set("hbase.hregion.majorcompaction", "0");
//        configuration.set("hbase.regionserver.regionSplitLimit", "1");
//        configuration.set("dfs.client.socket-timeout", 
//        		PropertiesUtil.getValue("dfs.client.socket-timeout"));
//        configuration.set("hbase.regionserver.handler.count", 
//        		PropertiesUtil.getValue("hbase.regionserver.handler.count"));
//		configuration.set("hbase.zookeeper.property.clientPort",
//				PropertiesUtil.getValue("hbase.zookeeper.property.clientPort"));
//		HBaseUtils.create(configuration);
//		System.out.println("HBase配置初始化成功");
		
		
		
		System.out.println("ONEDATA平台启动完成...");
	}
	//初始化表格
	private static boolean initTbales()
	{
		//postgresql
		//ONEDATA_DATAINFO
		if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"datainfo"))
		{
			String sql = "create table "+OneDataServer.TablePrefix+"datainfo"+"(id varchar(25) primary key,con varchar(250),layer varchar(60),innersql varchar(250),datatype varchar(15),oidfield varchar(50),geofield varchar(50),encode varchar(10),support varchar(50),"
					+ "indexpath varchar(150),hbasepath varchar(150),mapserver varchar(250),maptype varchar(15),remark varchar(250),regtime varchar(25),enable varchar(10),counts integer)";
			SystemDBHandle.exeSQLCreate(sql);
		}
		//ONEDATA_SHAREINFO
		if(!SystemDBHandle.isTableExist(OneDataServer.TablePrefix+"shareinfo"))
		{
			String sql = "create table "+OneDataServer.TablePrefix+"shareinfo"+"(id varchar(25) primary key,remark varchar(250),url varchar(500),operation varchar(10),token varchar(100),sharedate varchar(25))";
			SystemDBHandle.exeSQLCreate(sql);
		}
		//sqlite
		//ONEDATA_VISIT
		if(!SQLITEDBHandle.isTableExist(OneDataServer.TablePrefix+"visit"))
		{
			String sql = "create table "+OneDataServer.TablePrefix+"visit"+"(ID INTEGER PRIMARY KEY AUTOINCREMENT,REQ_TIME TEXT,REQ_IP TEXT,REQ_SERVER TEXT,REQ_INTERFACE TEXT,REQ_KEYWORD TEXT,TOKEN TEXT,USERTYPE TEXT,RESULTS TEXT,REMARKS TEXT)";
			SQLITEDBHandle.exeSQLCreate(sql);
		}
		//ONEDATA_TOKEN
		if(!SQLITEDBHandle.isTableExist(OneDataServer.TablePrefix+"token"))
		{
			String sql = "create table "+OneDataServer.TablePrefix+"token"+"(TOKEN_ID TEXT PRIMARY KEY,TOKEN_PHONE TEXT,TOKEN_COMPANY TEXT,TOKEN_NAME TEXT,TOKEN_TYPE TEXT,TOKEN_VALUE TEXT,TOKEN_STATE TEXT,TOKEN_DATE TEXT,TOKEN_NEWDATE TEXT)";
			SQLITEDBHandle.exeSQLCreate(sql);
		}
		return true;
	}
	//初始化服务节点
	private static void initServerNode()
	{
		String ServerNode = PropertiesUtil.getValue("SERVER_URL");
		//判断得到当前节点IP
		String suburl = ":8090/ONEDATA";
		String portproject = PropertiesUtil.getValue("PORTPROJECT");
		if(portproject != null && !"".equalsIgnoreCase(portproject))
		{
			suburl = portproject;
		}
		
		List<String>localIP = IPAddressUtil.getLocalIP();
		int size2 = localIP.size();
		for(int i=0;i<size2;i++)
		{
			String ip_m = localIP.get(i);
			ip_m += suburl;
			if(ServerNode.equalsIgnoreCase(ip_m)) 
			{
				CurrentServerNode = ip_m; 
				System.out.println("当前节点:"+CurrentServerNode);
			}
		}
		
		String flag = PropertiesUtil.getValue("BACKUP");
		System.out.println("是否开启备份:"+flag);
		if("true".equalsIgnoreCase(flag))
		{
			BackUp = true;
			ServerBackUp = PropertiesUtil.getValue("SERVER_URL_BACKUP");
			FilePathBackUp = PropertiesUtil.getValue("SERVER_FILEPATH_BACKUP");
		}
	}
	
	public static String getCurrentTime()
	{
		Date now = new Date();
		return df.format(now);
	}
	
	public static long getTime()
	{
		Date now = new Date();
		return now.getTime();
	}
	
	public static String getTableIndexPath(String table)
	{
		String indexpath = PropertiesUtil.getValue("SERVER_INDEXPATH");
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
	
	public static IndexSearcher getTableSeracher(String path)
	{
		if(tableReader.containsKey(path))
		{
			return  new IndexSearcher(tableReader.get(path));
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
					tableReader.put(path, reader);
					tableOrder.offer(path);
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

}
