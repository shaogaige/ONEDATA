/**
 * ClassName:POIDBToExcel.java
 * Date:2021年1月11日
 */
package com.idata.tool;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.ojdbc.sql.ConnectionManager;
import com.ojdbc.sql.ConnectionManager.ConnectionInfo;
import com.ojdbc.sql.ConnectionObject;
import com.ojdbc.sql.SQLResultSet;
import com.idata.core.DataBaseHandle;
import com.idata.core.OneDataServer;


/**
 * Creater:SHAO Gaige
 * Description:
 * Log:
 */
public class POIDBToExcel {
	
	/**
	 * 
	 * @param tableName 表名
	 * @param fileName 文件名
	 * @param field 条件字段
	 * @param value 条件值
	 * @return 返回生成的excel文件的路径
	 */
	public static String DB2Excel(String uid,String con,String tableName, String fileName,String field, String value,String operts) {
		//文件名称格式化
		// 获取配置文件中保存对应excel文件的路径
		String folderPath = PropertiesUtil.getValue("TEMPFILE_PATH");
		// 创建上传文件目录
		File folder = new File(folderPath);
		// 如果文件夹不存在创建对应的文件夹
		if (!folder.exists()) {
			folder.mkdirs();
		}
		//清理过期文件
		deleteUnableFile(folderPath);
		// 设置文件名
		fileName = fileName+RandomIDUtil.getDate("_")+".xls";
		
		String savePath = folderPath + "/" + fileName;
		ExcelOutput excel = new ExcelOutput(savePath);
		excel.createSheet("result", 0);
		
		//标题列名集合
		List<String> titleList = new ArrayList<String>();
		
		DataBaseHandle DBHandle = null;
		//获取数据库连接
		if(con != null && !"".equalsIgnoreCase(con))
		{
			try 
			{
				DBHandle = new DataBaseHandle(con);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		else if(uid != null && !"".equalsIgnoreCase(uid))
		{
			//使用ID获取数据库连接
			String sql = "select * from "+OneDataServer.TablePrefix+"databaseinfo where id='"+uid+"'";
			SQLResultSet rs = OneDataServer.SystemDBHandle.exeSQLSelect(sql);
			if(rs != null)
			{
				String conn = rs.getRow(0).getValue("conencode").getString_value();
				try 
				{
					DBHandle = new DataBaseHandle(conn);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
		}
		
		// 查询获取表中所有列名
		String sql1 = "select * from "+tableName;
		SQLResultSet rs1 = DBHandle.getMetaData(sql1);
		if (rs1 == null || rs1.getRowNum() < 1) {
			return null;
		}
		// 获取数据内容
        String sql2 = "select * from "+tableName+" where 1=1";
		
		if (field != null && !"".equalsIgnoreCase(field) && value != null && !"".equalsIgnoreCase(value)) 
		{
			String[] qfields = field.split(",");
			String[] qvalues = value.split(",");
			String[] opers = operts.split(",");
			for(int i=0;i<qfields.length;i++)
			{
				String qoper = null;
				if((i>opers.length-1) || "".equalsIgnoreCase(opers[i]))
				{
					qoper = "=";
				}
				else
				{
					qoper = opers[i];
				}
				
				if("id".equalsIgnoreCase(qfields[i]))
				{
					sql2 += " and "+qfields[i]+"='"+qvalues[i]+"'";
				}
				else
				{
					if(!"like".equalsIgnoreCase(qoper))
					{
						if("=".equalsIgnoreCase(qoper) || "!=".equalsIgnoreCase(qoper))
						{
							sql2 += " and "+qfields[i]+" "+qoper+"'"+qvalues[i]+"'";
						}
						else
						{
							sql2 += " and "+qfields[i]+" "+qoper+qvalues[i];
						}
					}
					else
					{
						sql2 += " and "+qfields[i]+" "+qoper+" '%"+qvalues[i]+"%'";
					}
				}
				
			}
		}
		// 执行查询到表中所有记录
		try {
			ConnectionInfo connInfo = DBHandle.getConnectionInfo();
			ConnectionObject conn = ConnectionManager.borrowConnectionObject(connInfo);
			Statement stat = conn.getConnection().createStatement();
			ResultSet rs2 = stat.executeQuery(sql2);
			//大集合放所有数据
			List<List<String>> list1 = new ArrayList<List<String>>();	
			//遍历结果集ResultSet
			while (rs2.next()) {
				//小集合放每一行数据
				List<String> list2 = new ArrayList<String>();
				for (int i = 0; i < rs1.getRowNum(); i++) {					
					//获取列名
					String columnName = rs1.getRow(i).getValue("name").getString_value();
					String columnValue = rs2.getString(columnName);
					list2.add(columnValue);
				}
				list1.add(list2);
			}
			
			//数据库查询获取注释作为标题名
			//String sql3 = "SELECT u.comments FROM user_tab_columns c ,user_col_comments u where u.table_name=c.TABLE_NAME and u.column_name=c.COLUMN_NAME and c.TABLE_NAME= '" + tableName + "'" + " order by c.TABLE_NAME,c.COLUMN_ID";
			//SQLResultSet rs3 = null;
			//Server.SystemDBHandle.exeSQLSelect(sql3);
			//如果有注释
//			if (rs3 != null && rs3.getRowNum() >= 1) {
//				for (int i = 0; i < rs3.getRowNum(); i++) {
//					String titleName = rs3.getRow(i).getValue("COMMENTS").getString_value();
//					//具体到单列的注释存在
//					if (titleName != null) {
//						titleList.add(titleName);
//					}else {
//						//其中某些列不存在注释用列名
//						titleList.add(rs1.getRow(i).getValue("COLUMN_NAME").getString_value());
//					}					
//				}
//			}else {
//				//表不存在列注释
//				for (int i = 0; i < rs1.getRowNum(); i++) {
//					titleList.add(rs1.getRow(i).getValue("name").getString_value());
//				}
//			}
			
			for (int i = 0; i < rs1.getRowNum(); i++) {
				titleList.add(rs1.getRow(i).getValue("name").getString_value());
			}
			
			// 把标题行写入excel文件中
			for (int i = 0; i < titleList.size(); i++) {
				excel.addLabel(0, i, titleList.get(i));
			}
			// 把取得的数据写入excel文件中
			for (int i = 0; i < list1.size(); i++) {
				//System.out.println(list1.size());
				// 创建list1.size()行数据
				// 把值一一写进单元格里
				// 设置第一列为自动递增的序号
				//row.createCell(0).setCellValue(i + 1);
				//从第二列开始添加数据,并且排除了数据库的ID列
				for (int j = 0; j < list1.get(i).size(); j++) {
					
					//String id = "";
					//if("id".equalsIgnoreCase(titleList.get(j)))
					//{
					//	id = list1.get(i).get(j);
					//}
					
					if("qr".equalsIgnoreCase(titleList.get(j)))
					{
//						String url = Server.QrURL+id+"&TYPE=public";
//						byte[] qr =null;
//						qr = QRUtil.getQRimage(id,url,"png");
//						//System.out.println(qr);
//						excel.addImage(i+1, j, qr, 500);
						//continue;
					}
					else
					{
						excel.addLabel(i+1, j, list1.get(i).get(j));
					}
					
				}			
			} 
			rs2.close();
			stat.close();
			ConnectionManager.returnConnectionObject(conn);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		//输出流
		try 
		{
			excel.saveLabels();
			
			List<String> listKey = new ArrayList<String>();
			listKey.add(savePath);
			String zipFilePath = savePath.replace(".xls", ".zip");
			ZIPUtil.zip(listKey, zipFilePath);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		// 返回文件保存全路径
		return savePath.replace(".xls", ".zip");
	}
	
	private static Integer deleteUnableFile(String fromDir){
		File srcDir = new File(fromDir);
		if (!srcDir.exists()) {
			return 0;
		}
		File[] files = srcDir.listFiles();
		if (files == null || files.length <= 0) {
			return 0;
		}
		int l = 0;
		Date today = new Date();
		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				try {
					File ff = files[i];
			        long time=ff.lastModified();
				    Calendar cal=Calendar.getInstance();   
			        cal.setTimeInMillis(time);   
			        Date lastModified = cal.getTime();
			      //(int)(today.getTime() - lastModified.getTime())/86400000;
			        long days = getDistDates(today, lastModified);
			        if(days>=1000*60*60*24){
			        	System.out.println("delete:"+ff.getName());
			        	files[i].delete();
			        	l++;
			        }
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return l;
	}
	
	 /** 
     * @param startDate 
     * @param endDate 
     * @return 
     * @throws ParseException 
     */  
    private static long getDistDates(Date startDate,Date endDate)    
    {  
        long totalDate = 0;  
        Calendar calendar = Calendar.getInstance();  
        calendar.setTime(startDate);  
        long timestart = calendar.getTimeInMillis();  
        calendar.setTime(endDate);  
        long timeend = calendar.getTimeInMillis();  
        totalDate = timestart - timeend;  
        return totalDate;  
    }  

}
