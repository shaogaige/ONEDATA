package com.idata.server;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.idata.core.OneDataServer;
import com.idata.data.IDataDriver;

/**
 * Servlet implementation class Cache
 */
@WebServlet(description = "缓存接口", urlPatterns = { "/Cache" })
public class Cache extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Cache() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		//数据操作
		String operation = request.getParameter("operation");
		String key = request.getParameter("key");
		if(key != null) 
		{
			key = new String(key.getBytes("ISO8859-1"),"UTF-8");
			key = URLDecoder.decode(key,"UTF-8");
		}
		String value = request.getParameter("value");
		if(value != null) 
		{
			value = new String(value.getBytes("ISO8859-1"),"UTF-8");
			value = URLDecoder.decode(value,"UTF-8");
		}
		String type = request.getParameter("type");
		
		String result = "{\"state\":false,\"message\":\"operation参数填写错误\",\"size\":0,\"data\":[]}";
		if("get".equalsIgnoreCase(operation))
		{
			if(key == null || "".equalsIgnoreCase(key))
			{
				result = "{\"state\":false,\"message\":\"key参数填写错误\",\"size\":0,\"data\":[]}";
			}
			else
			{
				String r = OneDataServer.cache.getObject(key);
				if(r != null )
				{
					if(r.startsWith("[") && r.endsWith("]"))
					{
						result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":1,\"data\":"+r+"}";
					}
					else if(r.startsWith("{") && r.endsWith("}")) 
					{
						result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":1,\"data\":["+r+"]}";
					}
					else
					{
						result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":1,\"data\":\""+r+"\"}";
					}
				}
				else
				{
					result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":1,\"data\":\""+r+"\"}";
				}
				
			}
			
		}
		else if("put".equalsIgnoreCase(operation))
		{
			if(key == null || "".equalsIgnoreCase(key))
			{
				result = "{\"state\":false,\"message\":\"key参数填写错误\",\"size\":0,\"data\":[]}";
			}
			else
			{
				if(OneDataServer.cache.add(key,value))
				{
					result = "{\"state\":true,\"message\":\"数据修改成功\",\"size\":1,\"data\":[]}";
				}
				else
				{
					result = "{\"state\":false,\"message\":\"数据修改失败\",\"size\":0,\"data\":[]}";
				}
			}
		}
		else if("clean".equalsIgnoreCase(operation))
		{
			if("resultcache".equalsIgnoreCase(type))
			{
				OneDataServer.resultcache.removeall();
				
				result = "{\"state\":true,\"message\":\"缓存清除成功\",\"size\":1,\"data\":[]}";
			}
			else if("all".equalsIgnoreCase(type))
			{
				OneDataServer.resultcache.removeall();
				OneDataServer.cache.removeall();
				OneDataServer.databaseinfo.removeall();
				IDataDriver.resultSize.removeall();
				IDataDriver.metaInfos.removeall();
				result = "{\"state\":true,\"message\":\"缓存清除成功\",\"size\":1,\"data\":[]}";
			}
		}
		
		//设置头
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json;charset=utf-8");
		response.setHeader("Access-Control-Allow-Origin", "*");
		
		response.getOutputStream().write(result.getBytes("UTF-8"));
		response.getOutputStream().flush();
		response.getOutputStream().close();
			
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
