/**
 * ClassName:OneDataServer.java 
 * Date:2020年1月9日
 */
package com.idata.core;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.idata.data.DataBaseHandle;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;
import com.idata.tool.PropertiesUtil;
/**
 * Creater:SHAO Gaige
 * Description:服务初始化类
 * Log:
 */
public class OneDataServer {
	
	//数据库处理
    public static DataBaseHandle SystemDBHandle = null;
	//数据库表前缀
	public static String TablePrefix = "";
	//序列前缀
	public static String SEQPrefix = "";
	//日期格式化
	public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	//服务节点
	public static String CurrentIP = "localhost:8090/ONEDATA";
	public static String[] ServerNodes;
	public static Map<String,Integer> ServerIndex = new HashMap<String,Integer>();
	public static String[] FilePaths;
	//备份节点
    public static boolean BackUp = false;
	public static String ServerBackUp;
	public static String FilePathBackUp;
	//搜索节点
	public static String SearchServer;
	public static boolean isSearchNode = true;
	//日志路径
	public static String logPath;
	public static boolean log = false;
	//索引读写
    public static IndexWriter writer;
	public static IndexSearcher searcher;
	public static Analyzer analyzer;
	public static Map<String,IndexReader> tableReader = new ConcurrentHashMap<String,IndexReader>();
	public static Queue<String> tableOrder = new ConcurrentLinkedQueue<String>();
	//访问次数缓存
	public static Map<String,Long> visitCache = new ConcurrentHashMap<String,Long>();
	//空间索引
    public static SpatialContext ctx = SpatialContext.GEO;
	public static SpatialStrategy strategy;
	public static SpatialContextFactory fac = new SpatialContextFactory();
	
	//内部字符串AES加密秘钥
	public final static String AESKEY = "ONEDATASTRINGENCKEY";
	
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
		SystemDBHandle = new DataBaseHandle(url,name,password);
		//initTable();
		getServerNodes();
		SearchServer = PropertiesUtil.getValue("SEARCH_INDEX_SERVER");
		System.out.println("搜索服务节点:"+SearchServer);
		if(SearchServer.equalsIgnoreCase(CurrentIP))
		{
			isSearchNode = true;
		}
		String indexPath = PropertiesUtil.getValue("SEARCH_INDEX_PATH");
		System.out.println("INDEX Path: "+indexPath);
		try 
		{
			//创建Directory指定索引保存位置
			Directory dir = FSDirectory.open(Paths.get(indexPath));
			if(DirectoryReader.indexExists(dir))
			{
				IndexReader reader = DirectoryReader.open(dir);
				searcher = new IndexSearcher(reader);
			}
			//使用IK进行分词
			analyzer = new IKAnalyzer(true);
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			//基于Directory创建IndexWriter
			writer = new IndexWriter(dir, iwc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logPath = PropertiesUtil.getValue("LOGFILE_PATH");
		System.out.println("日志文件路径:"+logPath);
		File logfile = new File(logPath + "/log.txt");
		LogUtil.setLogFile(logfile);
		String systemlog = PropertiesUtil.getValue("SYSTEM_LOG");
		if("true".equalsIgnoreCase(systemlog))
		{
			log = true;
		}
		else
		{
			System.out.println("记录操作日志未开启");
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
		
		//空间索引
		int maxLevels = 11;
		SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
		strategy = new RecursivePrefixTreeStrategy(grid, "GEOMETRY_INDEX");
		
		System.out.println("ONEDATA平台启动完成...");
	}
	
	private static void getServerNodes()
	{
		String serverNodes = PropertiesUtil.getValue("SERVER_URL");
		ServerNodes = serverNodes.split(",");
		System.out.println("服务节点:"+serverNodes);
		int size = ServerNodes.length;
		for(int i=0;i<size;i++)
		{
			ServerIndex.put(ServerNodes[i], i);
		}
		//判断得到当前节点IP
		String suburl = ":8090/ONEDATA";
		String portproject = PropertiesUtil.getValue("PORTPROJECT");
		if(portproject != null && !"".equalsIgnoreCase(portproject))
		{
			suburl = portproject;
		}
		String initurl = PropertiesUtil.getValue("INITURL");
		if(initurl != null && !"".equalsIgnoreCase(initurl))
		{
			CurrentIP = initurl;
		}
		List<String>localIP = IPAddressUtil.getLocalIP();
		int size2 = localIP.size();
		for(int i=0;i<size2;i++)
		{
			String ip_m = localIP.get(i);
			ip_m += suburl;
			if(OneDataServer.ServerIndex.containsKey(ip_m)) 
			{
				CurrentIP = ip_m; 
				System.out.println("当前节点:"+CurrentIP);
			}
		}
		String filePaths = PropertiesUtil.getValue("SERVER_FILEPATH");
		FilePaths = filePaths.split(",");
		System.out.println("文件保存路径:"+filePaths);
		
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
	
	public static boolean reloadSystemIndex()
	{
		//读取配置
		PropertiesUtil.init();
		String indexPath = PropertiesUtil.getValue("SEARCH_INDEX_PATH");
		
		try {
			Directory dir = FSDirectory.open(Paths.get(indexPath));
			IndexReader reader = DirectoryReader.open(dir);
			searcher = new IndexSearcher(reader);
			
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static IndexSearcher getTableSeracher(String tableName)
	{
		if(tableReader.containsKey(tableName))
		{
			return  new IndexSearcher(tableReader.get(tableName));
		}
		else
		{
			try 
			{
				String indexPath = PropertiesUtil.getValue("TABLES_INDEX_PATH") + "/" + tableName.trim();
				Directory dir = FSDirectory.open(Paths.get(indexPath));
				if(DirectoryReader.indexExists(dir))
				{
					IndexReader reader = DirectoryReader.open(dir);
					IndexSearcher searcher = new IndexSearcher(reader);
					tableReader.put(tableName, reader);
					tableOrder.offer(tableName);
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
