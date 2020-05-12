package com.idata.server;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.idata.cotrol.ConnectionControl;
import com.idata.cotrol.RegInterfaceControl;
import com.idata.tool.RandomIDUtil;

/**
 * Servlet implementation class Process
 */
@WebServlet(description = "其他通用接口", urlPatterns = { "/Process" })
public class Process extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Process() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		String type = request.getParameter("type");
		//数据操作
		String operation = request.getParameter("operation");
		String data = request.getParameter("data");
		if(data != null) {
			data = new String(data.getBytes("ISO8859-1"),"UTF-8");
			data = URLDecoder.decode(data,"UTF-8");
		}
		//表名
		String layer = request.getParameter("layer");
		if(layer != null) {
			layer = new String(layer.getBytes("ISO8859-1"),"UTF-8");
			layer = URLDecoder.decode(layer,"UTF-8");
		}
		String field = request.getParameter("field");
		if(field == null || "".equalsIgnoreCase(field))
		{
			field = "name";
		}
		String value = request.getParameter("value");

		// 定义分页参数默认值
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
		
		String result = "";
		if("connection".equalsIgnoreCase(type))
		{
			if(operation == null || "".equalsIgnoreCase(operation))
			{
				operation = "encode";
			}
			ConnectionControl conControl = new ConnectionControl();
			if("encode".equalsIgnoreCase(operation))
			{
			    result = conControl.getEncodeConStr(data);
			}
			else if("decode".equalsIgnoreCase(operation))
			{
				result = conControl.getDecodeConStr(data);
			}
			else if("check".equalsIgnoreCase(operation))
			{
				result = conControl.check(data, layer);
			}
			
		}
		else if("shareinfo".equalsIgnoreCase(type) || "datainfo".equalsIgnoreCase(type))
		{
			RegInterfaceControl regInterCon = new RegInterfaceControl();
			result = regInterCon.process(type, operation, data, field, value, start, count);
		}
		else if("uuid".equalsIgnoreCase(type))
		{
			String uuid = RandomIDUtil.getUUID("");
			result = "{\"state\":true,\"message\":\"数据查询成功\",\"size\":"+1+",\"data\":"+uuid+"}";
		}
		
		
		
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json;charset=utf-8");
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.getWriter().write(result);
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
