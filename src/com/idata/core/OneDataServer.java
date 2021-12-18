/**
 * ClassName:OneDataServer.java 
 * Date:2020年1月9日
 */
package com.idata.core;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.lucene.index.IndexReader;

import com.idata.tool.ActiveMQUtil;
import com.idata.tool.ClassUtil;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;
import com.idata.tool.RedisUtil;
import com.idata.tool.TWBCacheManager;
import com.idata.core.DataBaseHandle;
import com.idata.data.IDataDriver;
import com.idata.tool.RabbitMQUtil;
/**
 * Creater:SHAO Gaige
 * Description:服务初始化类
 * Log:
 */
public class OneDataServer {
	
	//数据库工具data&share
    public static DataBaseHandle SystemDBHandle = null;
    //内置访问记录SQLite数据库
    public static DataBaseHandle SQLITEDBHandle = null;
    //Hbase数据库
    public static HbaseHandle hbaseHandle = null;
    //Redis数据库(多数用于缓存)
    public static RedisUtil  redisUtil = null;
	//数据库表前缀
	public static String TablePrefix = "";
	//序列前缀
	public static String SEQPrefix = "";
	//服务节点
	public static String CurrentServerNode = "localhost:8090/onedata";
	//索引读写
	public static Map<String,IndexReader> tableReader = new ConcurrentHashMap<String,IndexReader>();
	public static Queue<String> tableOrder = new ConcurrentLinkedQueue<String>();
	//备份节点
    public static boolean BackUp = false;
	public static String ServerBackUp;
	public static String FilePathBackUp;
	//访问日志
	public static boolean visitlog = false;
	//消息队列
	public static boolean isActiveMQ = false;
	public static boolean isRabbitMQ = false;
	//token
	public static boolean checkToken = false;
	public static boolean doFilter = false;
	//异常日志路径
	public static String logPath;
	//日期格式化
	public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	//内部字符串AES加密秘钥
	public final static String AESKEY = "ONEDATASTRENCKEY";
	//文件传输状态
	public static TWBCacheManager<String> filestate = new TWBCacheManager<String>();
	//缓存接口
	public static TWBCacheManager<String> cache = new TWBCacheManager<String>(200,50,100);
	//查询结果缓存
	public static TWBCacheManager<String> resultcache = new TWBCacheManager<String>(100,20,10);
	//数据库信息缓存
	public static TWBCacheManager<SuperObject> databaseinfo = new TWBCacheManager<SuperObject>();
	//token缓存
	public static TWBCacheManager<Boolean> tokencache = new TWBCacheManager<Boolean>();
	//线程组
	public static TWBCacheManager<Thread> threads = new TWBCacheManager<Thread>();
	
	//系统初始化
	public static void init()
	{
		System.out.println("ONEDATA平台正在启动中...");
		//读取配置文件
		PropertiesUtil.init();
		//异常日志
		logPath = PropertiesUtil.getValue("LOGFILE_PATH");
		System.out.println("异常日志文件路径:"+logPath+"/log.txt");
		File logfile = new File(logPath + "/log.txt");
		LogUtil.setLogFile(logfile);
				
		TablePrefix = PropertiesUtil.getValue("TABLE_PREFIX");
		SEQPrefix = PropertiesUtil.getValue("SEQ_PREFIX");
		String url = PropertiesUtil.getValue("DataBaseURL");
		String name = PropertiesUtil.getValue("UserName");
		String password = PropertiesUtil.getValue("Password");
		try 
		{
			//连接数据库
			SystemDBHandle = new DataBaseHandle(url,name,password);
			//内置SQlite数据库
			SQLITEDBHandle = new DataBaseHandle("jdbc:sqlite://"+logPath+"/visitdata.db",null,null);
			System.out.println("访问记录路径:"+logPath+"/visitdata.db");
			//初始化表
			SystemManager.initTables();
			//支持类型
			List<Class<?>> subClass = ClassUtil.getClasses("com.idata.data", IDataDriver.class);
			System.out.println("支持类型："+subClass.size());
			for(int i=0;i<subClass.size();i++)
			{
				Class<?> d = subClass.get(i);
				//反射调用方法
				IDataDriver driver = GodTool.newInstanceDataDriver(d);
				driver.isSupport();
			}
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			LogUtil.info("配置的数据库连接不可用！");
			LogUtil.error(e1);
		}
		//访问日志
		String vlog = PropertiesUtil.getValue("VISIT_LOG");
		String rm = PropertiesUtil.getValue("RMQSUPPORT");
		String am = PropertiesUtil.getValue("ACTIVEMQ");
		if("true".equalsIgnoreCase(vlog))
		{
			visitlog = true;
			System.out.println("访问日志开启...");
			if("true".equalsIgnoreCase(am))
			{
				try
				{
					isActiveMQ = true;
					ActiveMQUtil.initConsumer();
					ActiveMQUtil.initProducer();
					System.out.println("内置ActiveMQ开启...");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					LogUtil.info("内置ActiveMQ初始化失败！");
					e.printStackTrace();
					LogUtil.error(e);
				}
			}
			else if("true".equalsIgnoreCase(rm))
			{
				try 
				{
					isRabbitMQ = true;
					RabbitMQUtil.initConsumer();
					RabbitMQUtil.initProducer();
					System.out.println("RabbitMQ开启...");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					LogUtil.info("RabbitMQ连接失败！");
					LogUtil.error(e);
				}
				
			}
			else
			{
				System.out.println("MQ消息队列中间件未开启！");
			}
		}
		else
		{
			System.out.println("访问日志未开启!");
		}
		//token授权
		String token = PropertiesUtil.getValue("CheckToken");
		if("true".equalsIgnoreCase(token))
		{
			System.out.println("token权限验证开启!");
			checkToken = true;
		}
		//filter检查
		String filter = PropertiesUtil.getValue("DOFILTER");
		if("true".equalsIgnoreCase(filter))
		{
			System.out.println("Filter验证开启!");
			doFilter = true;
		}
		//初始化服务节点
		initServerNode();
		//初始化索引路径
		String indexPath = PropertiesUtil.getValue("TABLES_INDEX_PATH");
		System.out.println("Index Path: "+indexPath);
		//初始化Hbase
		String hbasesupport = PropertiesUtil.getValue("HBASESUPPORT");
		if("true".equalsIgnoreCase(hbasesupport))
		{
			SystemManager.initHbase();
		}
		//初始化Redis
		String redissupport = PropertiesUtil.getValue("REDISSUPPORT");
		if("true".equalsIgnoreCase(redissupport))
		{
			SystemManager.initRedis();
		}
		
		LogUtil.info("ONEDATA平台启动完成...");
	}
	
	//初始化服务节点
	private static void initServerNode()
	{
		String ServerNode = PropertiesUtil.getValue("SERVER_URL");
		//判断得到当前节点IP
		String suburl = ":8090/onedata";
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
	
	public static String getYear()
	{
		Calendar now = Calendar.getInstance();
		int year = now.get(Calendar.YEAR);
		return String.valueOf(year);
	}
	
	public static String getMonth()
	{
		Calendar now = Calendar.getInstance();
		int month = now.get(Calendar.MONTH) + 1;
		return String.valueOf(month);
	}
	

}
