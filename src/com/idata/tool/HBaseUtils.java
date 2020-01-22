/**  
 * ClassName: HBaseDeom.java  
 * @date 2019年8月9日  
 */  
package com.idata.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**  
 * Creater:YANG Fan
 * Description: 
 * Log:
 */
public class HBaseUtils {
	private static final Logger logger = Logger.getLogger(HBaseUtils.class);
	private static HBaseUtils hBaseUtils = null;
	public static Connection connection = null;
	public static Admin admin = null;
	
	/**
	 * 单例模式之懒汉模式
	 * @param
	 * @return HBaseUtilsTest object
	 */
	public static HBaseUtils create(Configuration configuration) {
	    if (hBaseUtils == null) {
	        synchronized (HBaseUtils.class) {
	            if (hBaseUtils == null) {
	                hBaseUtils = new HBaseUtils(configuration);
	            }
	        }
	    }
	    return hBaseUtils;
	}
	
	/**
	 * @param
	 */
	private HBaseUtils(Configuration configuration){
	
	    //init data
	    try {
	        connection = ConnectionFactory.createConnection(configuration);
	        admin = connection.getAdmin();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	/**
	 * list all tables
	 */
	public static void listTables(){
	    try {
	        //获取所有的表名
	        TableName [] tableNames = admin.listTableNames();
	
	        if (tableNames.length == 0){
	            logger.info("HBase has no table");
	        }else {
	            for (TableName tableName:tableNames){
	                System.out.println("tableName:"+tableName.toString());
	            }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	/**
	 * read table
	 * tname:table name
	 * return:table context
	 */
	public static void readTable(String tname){
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        Boolean flag = admin.tableExists(tableName);
	        if (flag){
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	
	            for (Result result:resultScanner){
	                for (Cell cell:result.listCells()){
	                    //取行健
	                    String rowKey=Bytes.toString(CellUtil.cloneRow(cell));
	                    //取到时间戳
	                    long timestamp = cell.getTimestamp();
	                    //取到族列
	                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
	                    //取到修饰名
	                    String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
	                    //取到值
	                    String value = Bytes.toString(CellUtil.cloneValue(cell));
	
	                    System.out.println(" ====> RowKey : " + rowKey + ",  Timestamp : " +
	                            timestamp + ", ColumnFamily : " + family + ", Key : " + qualifier
	                            + ", Value : " + value);
	                }
	            }
	            resultScanner.close();
	        }else {
	            logger.error("Table is not exist");
	            System.exit(1);
	        }
	
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	
	/**
	 * read key value
	 * tname:table name
	 * return:T
	 */
	public static byte [] readKeyValue (String tname,String rKey,String cFamily, String qualifier){
	    //To String convert to TableName type
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag_1 = false;
	    boolean flag_2 = true;
	    boolean flag_3 = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag_1 = admin.tableExists(tableName);
	        if (flag_1) {
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            //get table object
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	            //判断当前输入的RowKey是否存在
	            if ("".equals(rKey) || " ".equals(rKey) || rKey == null){
	                logger.error("RowKey is null");
	                System.exit(1);
	            }else {
	                //判断列簇是否存在
	                for (Result result:resultScanner){
	                    for (Cell cell:result.listCells()){
	                        if (rKey.equals(Bytes.toString(CellUtil.cloneRow(cell)))){
	                            flag_1 = false;
	                            if (cFamily.equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
	                                flag_2 = false;
	                                flag_3 = qualifier.equals(Bytes.toString(CellUtil.cloneQualifier(cell)));
	                                if (flag_3) {
	                                    return CellUtil.cloneValue(cell);
	                                }
	                            }
	                        }
	                    }
	                }
	                if (flag_1){
	                    logger.error("Table has no specific RowKey");
	                    System.exit(1);
	                }
	
	                if (flag_2) {
	                    logger.error("Table has no specific column family");
	                    System.exit(1);
	                }
	
	                if (!flag_3) {
	                    logger.error("Table has no specific column");
	                    System.exit(1);
	                }
	            }
	        }else {
	            logger.error("Table is not exist");
	            System.exit(1);
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return null;
	}
	
	
	/**
	 * create table
	 * tname:table name
	 * return:Boolean
	 */
	public static Boolean createTable(String tname,String... cfamily){
	
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    Boolean flag = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag = admin.tableExists(tableName);
	        if (flag) {
	            logger.error("Table has existed");
	            return !flag;
	        }else {
	            //判断当前的表是否被禁用了,是就开启
	            /*if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }*/
	            HTableDescriptor hbaseTable = new HTableDescriptor(tableName);
	
	            //通过可变参数来传递不定量的列簇
	            for (String cf:cfamily){
	                hbaseTable.addFamily(new HColumnDescriptor(cf));
	            }
	            admin.createTable(hbaseTable);
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return !flag;
	}
	
	/**
	 * 
	 * Title: writeSingleRecord
	 * Description: 写入记录,不存在就是写入,已存在就更新
	 * Date:2019年10月14日 
	 * @param tname 表名 
	 * @param rKey rowkey(相当于主键)
	 * @param cFamily(列簇)
	 * @param qualifier(字段名)
	 * @param value
	 * @return
	 */
	public static boolean writeSingleRecord(String tname,String rKey,String cFamily, String qualifier, Object value){
	
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag = admin.tableExists(tableName);
	        if (flag) {
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	            //判断当前输入的Column Family是否存在
	            for (Result result:resultScanner){
	                for (Cell cell:result.listCells()){
	                    flag = cFamily.equals(Bytes.toString(CellUtil.cloneFamily(cell)));
	                    if (flag) break;
	                }
	            }
	            if (flag){
	                Put put = new Put(rKey.getBytes());
	                put.addColumn(Bytes.toBytes(cFamily), Bytes.toBytes(qualifier),Bytes.toBytes(value.toString()));
	                table.put(put);
	            }else {
	                logger.error("Column Family is not exist");
	                return false;
	            }
	        }else {
	            logger.error("Table is not exist");
	            return false;
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return flag;
	}
	
	/**
	 * 写入记录,不存在就是写入,已存在就更新
	 * tname:table name
	 * return:Boolean
	 */
	public static boolean writeMultipleRecord(String tname, String rKey, String cFamily, Map<String,Object> map){
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag = admin.tableExists(tableName);
	        if (flag) {
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	            //判断当前输入的Column Family是否存在
	            for (Result result:resultScanner){
	                for (Cell cell:result.listCells()){
	                    flag = cFamily.equals(Bytes.toString(CellUtil.cloneFamily(cell)));
	                    if (flag) break;
	                }
	            }
	            if (flag){
	                Put putValue = new Put(rKey.getBytes());
	                //put multiple
	                Set<String> keySet = map.keySet();
	                for (String key:keySet) {
	                    putValue.addColumn(Bytes.toBytes(cFamily),Bytes.toBytes(key),Bytes.toBytes(map.get(key).toString()));
	                }
	
	                table.put(putValue);
	            }else {
	                logger.error("Column Family is not exist");
	                return false;
	            }
	        }else {
	            logger.error("Table is not exist");
	            return false;
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return flag;
	}
	
	/**
	 * drop table
	 */
	public static boolean deleteTable(String tname){
	
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag = admin.tableExists(tableName);
	        if (flag) {
	            //禁用表
	            admin.disableTable(tableName);
	            //删除表
	            admin.deleteTable(tableName);
	        }else {
	            logger.error("Table is not exist");
	            return flag;
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return true;
	}
	
	/**
	 * delete row key
	 */
	public static boolean deleteRows(String tname,String... rKey){
	
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag = admin.tableExists(tableName);
	        if (flag) {
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            //get table object
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	            //判断当前输入的RowKey是否存在
	            for (Result result:resultScanner){
	                for (Cell cell:result.listCells()){
	                    flag = Arrays.asList(rKey).contains(Bytes.toString(CellUtil.cloneRow(cell)));
	                    if (flag) break;
	                }
	            }
	            if (flag){
	                //存放要被删除的RowKey的对象
	                List<Delete> list = new ArrayList<Delete>();
	                //逐条添加RowKey的对象
	                for (String rk:rKey){
	                    list.add(new Delete(Bytes.toBytes(rk)));
	                }
	                //批量删除
	                table.delete(list);
	            }else {
	                logger.error("Table has no specific RowKey");
	                return false;
	            }
	        }else {
	            logger.error("Table is not exist");
	            return false;
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return flag;
	}
	
	/**
	 * delete columns family
	 */
	public static boolean deleteColumnFamilys(String tname,String rKey,String... cFamily){
	
	    //To String convert to TableName
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag_1 = false;
	    boolean flag_2 = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag_1 = admin.tableExists(tableName);
	        if (flag_1) {
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            //get table object
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	            //判断当前输入的RowKey是否存在
	            if ("".equals(rKey) || " ".equals(rKey) || rKey == null){
	                logger.error("RowKey is null");
	                return false;
	            }else {
	                //判断列簇是否存在
	                for (Result result:resultScanner){
	                    for (Cell cell:result.listCells()){
	                        if (rKey.equals(Bytes.toString(CellUtil.cloneRow(cell)))){
	                            flag_1 = false;
	                            flag_2 = Arrays.asList(cFamily).contains(Bytes.toString(CellUtil.cloneFamily(cell)));
	                            if (flag_2) break;
	                        }
	                    }
	                }
	                if (flag_1){
	                    logger.error("Table has no specific RowKey");
	                    return false;
	                }
	
	                if (!flag_2) {
	                    logger.error("Table has no specific column family");
	                    return false;
	                }
	
	                if(!flag_1 && flag_2){
	                    //存放要被删除的RowKey的对象
	                    List<Delete> list = new ArrayList<Delete>();
	                    //获取RowKey的删除对象
	                    Delete delete = new Delete(Bytes.toBytes(rKey));
	                    //逐条添加cFamily的对象
	                    for (String cf:cFamily){
	                        list.add(delete.addFamily(Bytes.toBytes(cf)));
	                    }
	                    //批量删除
	                    table.delete(list);
	                }
	            }
	        }else {
	            logger.error("Table is not exist");
	            return false;
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return !flag_1 && flag_2;
	}
	
	/**
	 * delete columns
	 */
	public static boolean deleteColumns(String tname,String rKey,String cFamily,String... columns){
	
	    //To String convert to TableName type
	    TableName tableName = TableName.valueOf(tname);
	    boolean flag_1 = false;
	    boolean flag_2 = true;
	    boolean flag_3 = false;
	    try {
	        //判断tname是否存在,存在就返回true,否则返回false
	        flag_1 = admin.tableExists(tableName);
	        if (flag_1) {
	            //判断当前的表是否被禁用了,是就开启
	            if (admin.isTableDisabled(tableName)){
	                admin.enableTable(tableName);
	            }
	            //get table object
	            Table table = connection.getTable(tableName);
	
	            ResultScanner resultScanner = table.getScanner(new Scan());
	            //判断当前输入的RowKey是否存在
	            if ("".equals(rKey) || " ".equals(rKey) || rKey == null){
	                logger.error("RowKey is null");
	                return false;
	            }else {
	                //判断列簇是否存在
	                for (Result result:resultScanner){
	                    for (Cell cell:result.listCells()){
	                        if (rKey.equals(Bytes.toString(CellUtil.cloneRow(cell)))){
	                            flag_1 = false;
	                            if (cFamily.equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
	                                flag_2 = false;
	                                flag_3 = Arrays.asList(columns).contains(Bytes.toString(CellUtil.cloneQualifier(cell)));
	                                if (flag_3) break;
	                            }
	                        }
	                    }
	                }
	                if (flag_1){
	                    logger.error("Table has no specific RowKey");
	                    return false;
	                }
	
	                if (flag_2) {
	                    logger.error("Table has no specific column family");
	                    return false;
	                }
	
	                if (!flag_3) {
	                    logger.error("Table has no specific column");
	                    return false;
	                }
	
	                if(!flag_1 && !flag_2 && flag_3){
	                    //存放要被删除的RowKey的对象
	                    List<Delete> list = new ArrayList<Delete>();
	                    //获取RowKey的删除对象
	                    Delete delete = new Delete(Bytes.toBytes(rKey));
	                    //逐条添加cFamily的对象
	
	                    for (String c:columns){
	                        list.add(delete.addColumn(Bytes.toBytes(cFamily),Bytes.toBytes(c)));
	                    }
	                    //批量删除
	                    table.delete(list);
	                }
	            }
	        }else {
	            logger.error("Table is not exist");
	            return false;
	        }
	    }catch (IOException e) {
	        e.printStackTrace();
	    }
	    return !flag_1 && !flag_2 && flag_3;
	}
	
	
	/**
	 * Amount method
	 * The {@link Durability} is defaulted to {@link Durability#SYNC_WAL}.
	 * @param tname table name
	 * @param rKey The row that contains the cell to increment.
	 * @param cFamily The column family of the cell to increment.
	 * @param qualifier The column qualifier of the cell to increment.
	 * @param amount The amount to increment the cell with (or decrement, if the amount is negative).
	 * @return The new value, post increment.
	 */
	public long incrementColumnValue (String tname,String rKey,String cFamily,String qualifier,long amount){
	    long finalAmount = 0l;
	    try {
	        Table table = connection.getTable(TableName.valueOf(tname));
	
	        finalAmount = table.incrementColumnValue(
	                Bytes.toBytes(rKey),
	                Bytes.toBytes(cFamily),
	                Bytes.toBytes(qualifier.toString()),
	                amount
	        );
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    return finalAmount;
	}

	/**  
	 * Title: queryByValue
	 * Description: 根据value/rowkey前缀(空间数据)查询值
	 * Date:2019年10月10日 
	 * @param table 表名
	 * @param prefix rowkey前缀
	 * @param value	属性值
	 * @param rowkeyList 
	 * @param islike 
	 * @return
	 */  
	public static String queryByValue(String table, String value, List<String> rowkeyList, String islike) {
		try {
			//创建table对象
			Table tableName = connection.getTable(TableName.valueOf(table));
			
			Scan s = new Scan();
			
			FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
			if (value != null && !"".equalsIgnoreCase(value)) {
				
				//通过属性值查询
				if ("0".equalsIgnoreCase(islike)) {
					Filter filter = new ValueFilter(CompareOp.EQUAL,new RegexStringComparator(value)); 
					filterList.addFilter(filter);
				} else {
					Filter filter = new ValueFilter(CompareOp.EQUAL,new SubstringComparator(value)); 
					filterList.addFilter(filter);
				}
			}
			
			for (String rowkey : rowkeyList) {
				Filter filter = new PrefixFilter(rowkey.getBytes());
				filterList.addFilter(filter);
			}
			
			s.setFilter(filterList);
			ResultScanner rss = tableName.getScanner(s);
			
			JsonObject jsonObject = new JsonObject();
			List<Get> listGet = new ArrayList<>();
			for(Result r:rss){
				System.out.println("\n rowkey: "+new String(r.getRow()));
				listGet.add(new Get(r.getRow()));
			}
			
			JsonArray ja = new JsonArray();
			//根据rowkey得到下面值
			Result[] results = tableName.get(listGet);
			for (Result result : results) {
				JsonObject jo = new JsonObject();
				for (Cell kv1 : result.rawCells()) {
			    	 
					String key = Bytes.toString(CellUtil.cloneQualifier(kv1));
					String value1 = Bytes.toString(CellUtil.cloneValue(kv1));
					jo.addProperty(key, value1);
				}
				ja.add(jo);
			}
			jsonObject.add("data", ja);
			jsonObject.addProperty("state", true);
			jsonObject.addProperty("message", "查询成功");
			System.out.println(jsonObject);
			rss.close();
			return jsonObject.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static byte[] getUTF8BytesFromGBKString(String gbkStr) {
		int n = gbkStr.length();
		byte[] utfBytes = new byte[3 * n];
		int k = 0;
		for (int i = 0; i < n; i++) {
			int m = gbkStr.charAt(i);
			if (m < 128 && m >= 0) {
				utfBytes[k++] = (byte) m;
				continue;
			}
			utfBytes[k++] = (byte) (0xe0 | (m >> 12));
			utfBytes[k++] = (byte) (0x80 | ((m >> 6) & 0x3f));
			utfBytes[k++] = (byte) (0x80 | (m & 0x3f));
		}
		if (k < utfBytes.length) {
			byte[] tmp = new byte[k];
			System.arraycopy(utfBytes, 0, tmp, 0, k);
			return tmp;
		}
		return utfBytes;
	}
}
