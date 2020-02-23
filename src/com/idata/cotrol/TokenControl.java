/**
 * ClassName:TokenControl.java
 * Date:2020年1月28日
 */
package com.idata.cotrol;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import com.idata.core.OneDataServer;
import com.idata.core.ResultBuilder;
import com.idata.core.SuperObject;
import com.idata.tool.DateCoder;
import com.idata.tool.ICoder;
import com.idata.tool.IPAddressCoder;
import com.idata.tool.NumberCoder;
import com.idata.tool.PhoneNumCoder;
import com.ojdbc.sql.PreparedParam;
import com.ojdbc.sql.SQLResultSet;
import com.ojdbc.sql.SQLRow;
import com.ojdbc.sql.Value;

/**
 * Creater:SHAO Gaige
 * Description:token授权管理类
 * Log:
 */
public class TokenControl {
	
	private ICoder typecoder = null;
	private DateCoder datecoder = new DateCoder();
	private PhoneNumCoder phonecoder = new PhoneNumCoder();
	
	public String process(String operation,String token,String name,String phone,String company,String type,String value,String state,String keyword,int start,int count)
	{
		if("get".equalsIgnoreCase(operation))
		{
			try 
			{
				String _token = "";
				_token += datecoder.code(null);
				if("I".equalsIgnoreCase(type))
				{
					//ip
					typecoder = new IPAddressCoder();
					_token += "I";
					_token += phonecoder.code(phone);
					_token += typecoder.code(value);
				}
				else if("D".equalsIgnoreCase(type))
				{
					//date
					_token += "D";
					_token += phonecoder.code(phone);
					_token += datecoder.code(value);
				}
				else if("N".equalsIgnoreCase(type))
				{
					//num
					typecoder = new NumberCoder();
					_token += "N";
					_token += phonecoder.code(phone);
					_token += typecoder.code(value);
				}
				else if("T".equalsIgnoreCase(type))
				{
					//临时测试
					_token += "T";
					Date date=new Date();
				    Calendar calendar = Calendar.getInstance();
				    calendar.setTime(date);
				    calendar.add(Calendar.DAY_OF_MONTH, +2);
				    date = calendar.getTime();
				    _token += datecoder.code(DateCoder.df.format(date));
				}
				
				if(checkID(_token))
				{
					//存在
					return _token;
				}
				
				//写入数据库
				//TOKEN_ID,TOKEN_PHONE,TOKEN_COMPANY,TOKEN_NAME,TOKEN_TYPE,TOKEN_VALUE,TOKEN_STATE,TOKEN_DATE,TOKEN_NEWDATE
				String sql = "insert into TOKEN_DATA (TOKEN_ID,TOKEN_PHONE,TOKEN_COMPANY,TOKEN_NAME,TOKEN_TYPE,TOKEN_VALUE,TOKEN_STATE,TOKEN_DATE,TOKEN_NEWDATE) values(?,?,?,?,?,?,?,?,?)";
		        PreparedParam preparedParam = new PreparedParam();
		        
		        Value value1 = new Value();
		        value1.setString_value(_token);
		        preparedParam.addParam(1, value1);
		        
		        Value value2 = new Value();
		        value2.setString_value(phone);
		        preparedParam.addParam(2, value2);
		        
		        Value value3 = new Value();
		        value3.setString_value(company);
		        preparedParam.addParam(3, value3);
		        
		        Value value4 = new Value();
		        value4.setString_value(name);
		        preparedParam.addParam(4, value4);
		        
		        Value value5 = new Value();
		        value5.setString_value(type);
		        preparedParam.addParam(5, value5);
		        
		        Value value6 = new Value();
		        value6.setString_value(value);
		        preparedParam.addParam(6, value6);
		        
		        Value value7 = new Value();
		        value7.setString_value("0");
		        preparedParam.addParam(7, value7);
		        
		        Value value8 = new Value();
		        value8.setString_value(DateCoder.df.format(new Date()));
		        preparedParam.addParam(8, value8);
		        
		        Value value9 = new Value();
		        value9.setString_value(DateCoder.df.format(new Date()));
		        preparedParam.addParam(9, value9);
		        
		        OneDataServer.SQLITEDBHandle.exePreparedSQLInsert(sql,preparedParam);
		        
				return _token;
			}
			catch(Exception e)
			{
				e.printStackTrace();
				return "error! please connect to website managment...";
			}
		}
		else if("query".equalsIgnoreCase(operation))
		{
			String sql = "";
			if(state != null && !"".equalsIgnoreCase(state))
			{
				if("0".equalsIgnoreCase(state))
				{
					sql = "select * from TOKEN_DATA where TOKEN_STATE='0'";
				}
				else if("1".equalsIgnoreCase(state))
				{
					sql = "select * from TOKEN_DATA where TOKEN_STATE='1'";
				}
				else
				{
					sql = "select * from TOKEN_DATA";
				}
				
				if(keyword != null && !"".equalsIgnoreCase(keyword))
				{
					if("0".equalsIgnoreCase(state) || "1".equalsIgnoreCase(state))
					{
						sql += " and ( TOKEN_ID like '%"+keyword+"%' or TOKEN_PHONE like '%"+keyword+"%' or TOKEN_COMPANY like '%"+keyword+"%' or TOKEN_NAME like '%"+keyword+"%' or TOKEN_VALUE like '%"+keyword+"%' or TOKEN_DATE like '%"+keyword+"%')";
					}
					else
					{
						sql += " where TOKEN_ID like '%"+keyword+"%' or TOKEN_PHONE like '%"+keyword+"%' or TOKEN_COMPANY like '%"+keyword+"%' or TOKEN_NAME like '%"+keyword+"%' or TOKEN_VALUE like '%"+keyword+"%' or TOKEN_DATE like '%"+keyword+"%'";
					}
					
				}
				
				//排序
				sql += " order by TOKEN_DATE desc";
				//System.out.println(sql);
			}
			else
			{
				if(token == null || token.length()<11)
				{
					return null;
				}
				if(token.length() != 11)
				{
					if(token.length() < 16)
					{
						return null;
					}
					String pdate = token.substring(0, 14);
					String _type = token.substring(14, 15);
					Date dates = datecoder.uncode2Date(pdate);
					Date now = new Date();
					if(dates.getTime()>now.getTime())
					{
						return null;
					}
					if(!"I".equalsIgnoreCase(_type) && !"D".equalsIgnoreCase(_type) && !"N".equalsIgnoreCase(_type)
							&& !"T".equalsIgnoreCase(_type))
					{
						return null;
					}
				}
				else if(token.length() == 11)
				{
					Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");  
			        if(!pattern.matcher(token).matches())
			        {
			        	return null;
			        }
				}
				sql = "select * from TOKEN_DATA where TOKEN_ID='"+token+"' or TOKEN_PHONE='"+token+"'";
			}
			List<SuperObject> r = OneDataServer.SQLITEDBHandle.exeSQLSelect2(sql,start,count,null,null);
	        if(r == null || r.size() < 1)
	        {
	        	return null;
	        }
			return ResultBuilder.object2string(r, "json");
		}
		else if("check".equalsIgnoreCase(operation))
		{
			//尽可能做的高效一些
			try
			{
				if(token == null || token.length() < 16)
				{
					return "-1";
				}
				String pdate = token.substring(0, 14);
				String _type = token.substring(14, 15);
				Date dates = datecoder.uncode2Date(pdate);
				Date now = new Date();
				if(dates.getTime()>now.getTime())
				{
					return "-1";
				}
				if(!"I".equalsIgnoreCase(_type) && !"D".equalsIgnoreCase(_type) && !"N".equalsIgnoreCase(_type)
						&& !"T".equalsIgnoreCase(_type))
				{
					return "-1";
				}
				
				//测试
				if("T".equalsIgnoreCase(_type))
				{
					String limitDate = token.substring(15);
					Date ldate = datecoder.uncode2Date(limitDate);
					if(ldate.getTime()>=now.getTime())
					{
						return "1";
					}
					else
					{
						return "-1";
					}
				}
				
				String sql = "select TOKEN_STATE from TOKEN_DATA where TOKEN_ID='"+token+"'";
		        SQLResultSet r = OneDataServer.SQLITEDBHandle.exeSQLSelect(sql);
		        if(r==null || r.getRowNum() < 1)
		        {
		        	return "-1";
		        }
		        
		        SQLRow row = r.getRow(0);
	        	Value v = row.getValue("TOKEN_STATE");
	        	String s = v.getString_value();
				
				return s;
				//缺少对token值深层次验证
			}
			catch(Exception e)
			{
				e.printStackTrace();
				return "-1";
			}
		}
		else if("pass".equalsIgnoreCase(operation))
		{
			String updateTime = DateCoder.df.format(new Date());
			String sql = "update TOKEN_DATA set TOKEN_STATE='1',TOKEN_NEWDATE='"+updateTime+"' where TOKEN_ID='"+token+"'";
			return String.valueOf(OneDataServer.SQLITEDBHandle.exeSQLUpdate(sql));
		}
		else if("cancel".equalsIgnoreCase(operation))
		{
			String updateTime = DateCoder.df.format(new Date());
			String sql = "update TOKEN_DATA set TOKEN_STATE='0',TOKEN_NEWDATE='"+updateTime+"' where TOKEN_ID='"+token+"'";
			return String.valueOf(OneDataServer.SQLITEDBHandle.exeSQLUpdate(sql));
		}
		else if("delete".equalsIgnoreCase(operation))
		{
			String sql = "delete from TOKEN_DATA where TOKEN_ID='"+token+"'";
			return String.valueOf(OneDataServer.SQLITEDBHandle.exeSQLDelete(sql));
		}
		return null;
		
	}
	
	private boolean checkID(String id)
	{
		String sql = "select TOKEN_STATE from TOKEN_DATA where TOKEN_ID='"+id+"'";
        SQLResultSet r = OneDataServer.SQLITEDBHandle.exeSQLSelect(sql);
        if(r==null || r.getRowNum() < 1)
        {
        	return false;
        }
        else
        {
        	return true;
        }
	}

}
