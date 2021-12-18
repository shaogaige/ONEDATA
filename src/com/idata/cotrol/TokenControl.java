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
import com.idata.tool.LogUtil;
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
public class TokenControl extends DataControl{
	
	private ICoder typecoder = null;
	private DateCoder datecoder = new DateCoder();
	private PhoneNumCoder phonecoder = new PhoneNumCoder();
	
	public String process(String operation,String token,String name,String phone,String company,String type,String value,String state,String keyword,int start,int count,
			String registerid,String registername,String databaseid,String databasename,String table,String _interface,String model)
	{
		String result = "{\"state\":false,\"message\":\"token操作失败！\",\"size\":0,\"data\":[]}";
		if("get".equalsIgnoreCase(operation))
		{
			result = get(name,phone,company,type,value,registerid,registername,databaseid,databasename,table,_interface,model);
		}
		else if("check".equalsIgnoreCase(operation))
		{
			boolean f = check(token);
			if(f)
			{
				result = "{\"state\":true,\"message\":\"token验证成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"token不存在或已过期\",\"size\":0,\"data\":[]}";
			}
		}
		else if("query".equalsIgnoreCase(operation))
		{
			List<SuperObject> rs = query(state,keyword,token,start,count);
			if(rs == null || rs.size() < 1)
	        {
	        	return "{\"state\":true,\"message\":\"数据查询成功\",\"size\":0,\"data\":[]}";
	        }
			int size = rs.size();
			String r = ResultBuilder.object2string(rs, "json");
			result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":"+size+",\"data\":"+r+"}";
		}
		else if("pass".equalsIgnoreCase(operation))
		{
			boolean f = pass(token);
			if(f)
			{
				result = "{\"state\":true,\"message\":\"token授权成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"token授权失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("cancel".equalsIgnoreCase(operation))
		{
			boolean f = cancel(token);
			if(f)
			{
				result = "{\"state\":true,\"message\":\"token取消授权成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"token取消授权失败\",\"size\":0,\"data\":[]}";
			}
		}
		else if("delete".equalsIgnoreCase(operation))
		{
			boolean f = check(token);
			if(f)
			{
				result = "{\"state\":true,\"message\":\"token删除成功\",\"size\":0,\"data\":[]}";
			}
			else
			{
				result = "{\"state\":false,\"message\":\"token删除失败\",\"size\":0,\"data\":[]}";
			}
		}
		return result;
		
	}
	
	public String get(String name,String phone,String company,String type,String value,String registerid,
			String registername,String databaseid,String databasename,String table,String _interface,String model)
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
				Date date = new Date();
			    Calendar calendar = Calendar.getInstance();
			    calendar.setTime(date);
			    calendar.add(Calendar.DAY_OF_MONTH, +2);
			    date = calendar.getTime();
			    _token += datecoder.code(DateCoder.df.format(date));
			}
			
			if(check(_token))
			{
				//存在
				return _token;
			}
			
			//写入数据库
			//TOKEN_ID,TOKEN_PHONE,TOKEN_COMPANY,TOKEN_NAME,TOKEN_TYPE,TOKEN_VALUE,TOKEN_STATE,TOKEN_DATE,TOKEN_NEWDATE
			String sql = "insert into "+OneDataServer.TablePrefix+"token (token_id,token_phone,token_company,token_name,token_type,token_value,token_state,token_date,token_newdate,"
					+ "register_id,register_name,database_id,database_conname,token_table,token_interface,token_model) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
	        
	        Value value10 = new Value();
	        value10.setString_value(registerid);
	        preparedParam.addParam(10, value10);
	        
	        Value value11 = new Value();
	        value11.setString_value(registername);
	        preparedParam.addParam(11, value11);
	        
	        Value value12 = new Value();
	        value12.setString_value(databaseid);
	        preparedParam.addParam(12, value12);
	        
	        Value value13 = new Value();
	        value13.setString_value(databasename);
	        preparedParam.addParam(13, value13);
	        
	        Value value14 = new Value();
	        value14.setString_value(table);
	        preparedParam.addParam(14, value14);
	        
	        Value value15 = new Value();
	        value15.setString_value(_interface);
	        preparedParam.addParam(15, value15);
	        
	        Value value16 = new Value();
	        value16.setString_value(model);
	        preparedParam.addParam(16, value16);
	        
	        OneDataServer.SystemDBHandle.exePreparedSQLInsert(sql,preparedParam);
	        
	        
	        String result = "{\"state\":true,\"message\":\"token获取成功\",\"size\":1,\"data\":[{\"token\":\""+_token+"\"}]}";
			return result;
		}
		catch(Exception e)
		{
			LogUtil.error(e);
			String result = "{\"state\":false,\"message\":\"error! please connect to website managment...\",\"size\":0,\"data\":[]}";
			return result;
		}
	}
	
	public List<SuperObject> query(String state,String keyword,String token,int start,int count) 
	{
		String sql = "";
		if(state != null && !"".equalsIgnoreCase(state))
		{
			if("0".equalsIgnoreCase(state))
			{
				sql = "select * from "+OneDataServer.TablePrefix+"token where token_state='0'";
			}
			else if("1".equalsIgnoreCase(state))
			{
				sql = "select * from "+OneDataServer.TablePrefix+"token where token_state='1'";
			}
			else
			{
				sql = "select * from "+OneDataServer.TablePrefix+"token";
			}
			
			if(keyword != null && !"".equalsIgnoreCase(keyword))
			{
				if("0".equalsIgnoreCase(state) || "1".equalsIgnoreCase(state))
				{
					sql += " and ( token_id like '%"+keyword+"%' or token_phone like '%"+keyword+"%' or token_company like '%"+keyword+"%' or token_name like '%"+keyword+"%' or token_value like '%"+keyword+"%' or token_date like '%"+keyword+"%')";
				}
				else
				{
					sql += " where token_id like '%"+keyword+"%' or token_phone like '%"+keyword+"%' or token_company like '%"+keyword+"%' or token_name like '%"+keyword+"%' or token_value like '%"+keyword+"%' or token_date like '%"+keyword+"%'";
				}
				
			}
			
			//排序
			sql += " order by token_date desc";
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
			sql = "select * from "+OneDataServer.TablePrefix+"token where token_id='"+token+"' or token_phone='"+token+"'";
		}
		List<SuperObject> r = OneDataServer.SystemDBHandle.exeSQLSelect2(sql,start,count,null,null);
		return r;
	}
	
	public boolean pass(String token)
	{
		String updateTime = DateCoder.df.format(new Date());
		String sql = "update "+OneDataServer.TablePrefix+"token set token_state='1',token_newdate='"+updateTime+"' where token_id='"+token+"'";
		OneDataServer.tokencache.removeContainKey(token);
		return OneDataServer.SystemDBHandle.exeSQLUpdate(sql);
	}
	
	public boolean cancel(String token)
	{
		String updateTime = DateCoder.df.format(new Date());
		String sql = "update "+OneDataServer.TablePrefix+"token set token_state='0',token_newdate='"+updateTime+"' where token_id='"+token+"'";
		OneDataServer.tokencache.removeContainKey(token);
		return OneDataServer.SystemDBHandle.exeSQLUpdate(sql);
	}
	
	public boolean delete(String token)
	{
		String sql = "delete from "+OneDataServer.TablePrefix+"token where token_id='"+token+"'";
		OneDataServer.tokencache.removeContainKey(token);
		return OneDataServer.SystemDBHandle.exeSQLDelete(sql);
	}

	public boolean check(String token)
	{
		//尽可能做的高效一些
		try
		{
			if(OneDataServer.tokencache.containsKey(token))
			{
				return OneDataServer.tokencache.getObject(token);
			}
			if(token == null || token.length() < 16)
			{
				return false;
			}
			String pdate = token.substring(0, 14);
			String _type = token.substring(14, 15);
			Date dates = datecoder.uncode2Date(pdate);
			Date now = new Date();
			if(dates.getTime()>now.getTime())
			{
				return false;
			}
			if(!"I".equalsIgnoreCase(_type) && !"D".equalsIgnoreCase(_type) && !"N".equalsIgnoreCase(_type)
					&& !"T".equalsIgnoreCase(_type))
			{
				return false;
			}
			
			//测试
			if("T".equalsIgnoreCase(_type))
			{
				String limitDate = token.substring(15);
				Date ldate = datecoder.uncode2Date(limitDate);
				if(ldate.getTime()>=now.getTime())
				{
					return true;
				}
				else
				{
					return false;
				}
			}
			
			String sql = "select token_state from "+OneDataServer.TablePrefix+"token where token_id='"+token+"'";
	        SQLResultSet r = OneDataServer.SystemDBHandle.exeSQLSelect(sql);
	        if(r==null || r.getRowNum() < 1)
	        {
	        	return false;
	        }
	        
	        SQLRow row = r.getRow(0);
        	Value v = row.getValue("token_state");
        	String s = v.getString_value();
			
        	if("1".equalsIgnoreCase(s))
        	{
        		OneDataServer.tokencache.add(token,true);
        		return true;
        	}
        	else
        	{
        		OneDataServer.tokencache.add(token,false);
        		return false;
        	}
		}
		catch(Exception e)
		{
			LogUtil.error(e);
			return false;
		}
	}

}
