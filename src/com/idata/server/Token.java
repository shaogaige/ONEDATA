package com.idata.server;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.idata.core.TokenTimeManager;
import com.idata.cotrol.TokenControl;

/**
 * Servlet implementation class Token
 */
@WebServlet(description = "授权秘钥", urlPatterns = { "/Token" })
public class Token extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Token() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		String state = request.getParameter("state");
		String keyword = request.getParameter("keyword");
		if(keyword != null) {
			keyword = new String(keyword.getBytes("ISO8859-1"),"UTF-8");
			keyword = URLDecoder.decode(keyword,"UTF-8");
		}
		
		String token = request.getParameter("token");
		
		String type = request.getParameter("type");
		String name = request.getParameter("name");
		if(name != null) {
			name = new String(name.getBytes("ISO8859-1"),"UTF-8");
			name = URLDecoder.decode(name,"UTF-8");
		}
		String phone = request.getParameter("phone");
		String company = request.getParameter("company");
		if(company != null) {
			company = new String(company.getBytes("ISO8859-1"),"UTF-8");
			company = URLDecoder.decode(company,"UTF-8");
		}
		String value = request.getParameter("value");
		if(value != null) {
			value = new String(value.getBytes("ISO8859-1"),"UTF-8");
			value = URLDecoder.decode(value,"UTF-8");
		}
		
		String operation = request.getParameter("operation");
		
		int start = 1, count = Integer.MAX_VALUE;
		// 获取分页参数,并赋值给分页变量
		String start1 = request.getParameter("start");
		if (start1 != null && !"".equalsIgnoreCase(start1))
		{
			start = Integer.parseInt(start1.trim());
		}
		String count1 = request.getParameter("count");
		if (count1 != null && !"".equalsIgnoreCase(count1))
		{
			count = Integer.parseInt(count1.trim());
		}
		
		if("get".equalsIgnoreCase(operation))
		{
			boolean f = checkParam(name,phone,company,type,value);
			if(!f)
			{
				//设置头
				response.setCharacterEncoding("UTF-8");
				response.setContentType("application/json;charset=utf-8");
				response.setHeader("Access-Control-Allow-Origin", "*");
				//参数有错误
				response.getWriter().write("{\"token\":\"\",\"message\":\"参数解析出现错误！\"}");
				return;
			}
			String key = name+phone+company+type;
			boolean t = TokenTimeManager.check(key);
			if(!t) 
			{
				//设置头
				response.setCharacterEncoding("UTF-8");
				response.setContentType("application/json;charset=utf-8");
				response.setHeader("Access-Control-Allow-Origin", "*");
				//参数有错误
				response.getWriter().write("{\"token\":\"\",\"message\":\"同一个账号信息在5分钟内多次申请！\"}");
				return;
			}
		}
		
		String result = "";
		TokenControl tokenControl = new TokenControl();
		result = tokenControl.process(operation,token,name,phone,company,type,value,state,keyword,start,count);
		
		//设置头
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json;charset=utf-8");
		response.setHeader("Access-Control-Allow-Origin", "*");
		
		response.getOutputStream().write(result.getBytes("UTF-8"));
		response.getOutputStream().flush();
		response.getOutputStream().close();		
	}
	
	private boolean checkParam(String name,String phone,String company,String type,String value)
	{
		if(name == null || "".equalsIgnoreCase(name) || name.length()<2)
		{
			return false;
		}
		
		if(phone == null || "".equalsIgnoreCase(phone) || phone.length()<11)
		{
			return false;
		}
		else
		{
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");  
	        boolean f = pattern.matcher(phone).matches();
	        if(!f) {
	        	return false;
	        }
		}
		
		if(company == null || "".equalsIgnoreCase(company) || company.length()<2)
		{
			return false;
		}
		
		if(type == null || "".equalsIgnoreCase(type) || (!"I".equalsIgnoreCase(type) && !"D".equalsIgnoreCase(type)
				&& !"N".equalsIgnoreCase(type) && !"T".equalsIgnoreCase(type))) {
			return false;
		}
		
//		if(value == null || "".equalsIgnoreCase(value))
//		{
//			return false;
//		}
		
		return true;
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
