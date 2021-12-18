package com.idata.server;

import java.io.IOException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.idata.core.OneDataServer;
import com.idata.core.SystemManager;
import com.idata.tool.LogUtil;

/**
 * Servlet implementation class OneDataWrapper
 */
@WebServlet(description = "启动接口", urlPatterns = { "/OneDataWrapper" })
public class OneDataWrapper extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public OneDataWrapper() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
		OneDataServer.init();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	
	@SuppressWarnings("deprecation")
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		String operation = request.getParameter("operation");
		String type = request.getParameter("type");
		String value = request.getParameter("value");
		if("manage".equalsIgnoreCase(operation))
		{
			if("stop".equalsIgnoreCase(type))
			{
				if("server".equalsIgnoreCase(value))
				{
					SystemManager.stopServer();
					//设置头
					response.setCharacterEncoding("UTF-8");
					response.setContentType("application/json;charset=utf-8");
					response.setHeader("Access-Control-Allow-Origin", "*");
					response.getWriter().write("{\"state\":true,\"message\":\"程序后台成功关闭...\",\"size\":0,\"data\":[]}");
					LogUtil.error("非法程序，已被服务器关闭使用权限...");
					return;
				}
			}
		}
		else if("thread".equalsIgnoreCase(operation))
		{
			if("stop".equalsIgnoreCase(type))
			{
				String result = "";
				if(OneDataServer.threads.containsKey(value))
				{
					Thread t = OneDataServer.threads.getObject(value);
					t.stop();
					boolean f = OneDataServer.threads.remove(value);
					if(f)
					{
						result = "{\"state\":true,\"message\":\"UUID的线程停止成功！\",\"size\":0,\"data\":[]}";
					}
					else
					{
						result = "{\"state\":false,\"message\":\"UUID的线程停止失败！\",\"size\":0,\"data\":[]}";
					}
				}
				else
				{
					result = "{\"state\":false,\"message\":\"UUID的线程不存在！\",\"size\":0,\"data\":[]}";
				}
				//设置头
				response.setCharacterEncoding("UTF-8");
				response.setContentType("application/json;charset=utf-8");
				response.setHeader("Access-Control-Allow-Origin", "*");
				//参数有错误
				response.getWriter().write(result);
				return;
			}
		}
		//设置头
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json;charset=utf-8");
		response.setHeader("Access-Control-Allow-Origin", "*");
		//参数有错误
		response.getWriter().write("{\"state\":false,\"message\":\"参数解析出现错误！\",\"size\":0,\"data\":[]}");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
