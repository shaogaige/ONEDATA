package com.idata.server;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.idata.core.DataParam;
import com.idata.core.OneDataServer;
import com.idata.core.SuperObject;
import com.idata.cotrol.VisitControl;
import com.idata.data.DataBaseDriver;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.PropertiesUtil;

/**
 * Servlet implementation class Login
 */
@WebServlet(description = "登录验证", urlPatterns = { "/Login" })
public class Login extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Login() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
        String username = request.getParameter("username");
        String password = request.getParameter("password");
        //String type = request.getParameter("type");
        String validationCode = request.getParameter("validationCode");
        HttpSession session = request.getSession();
        String validation_code = (String)session.getAttribute("validation_code");
        
        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html;charset=utf-8;");
        if(!validationCode.equalsIgnoreCase(validation_code)){
        	//System.out.println("验证码错误");
            //返回信息
        	response.getWriter().write("<script> alert(\"验证码输入错误！\");parent.location.href='/onedata/login.html';</script>");
        	return;
        }
        
        String result = checkUser(username,password,session);
        if (result.equals("hasUserNameAndPasswordCorrect")) {
             // System.out.println("用户名和密码均正确");
            //跳转到管理界面
            session.setAttribute("sid", session.getId());
            response.sendRedirect("/onedata/index.html");
            return;
        } else if (result.equals("hasUserNameButPasswordInCorrect")) {
            //System.out.println("用户名正确,密码不正确");
            //返回信息，提示错误
            response.getWriter().write("<script> alert(\"用户名正确,密码不正确！\");parent.location.href='/onedata/login.html';</script>");
            return;
        } else if (result.equals("hasNoUserName")) {
           // System.out.println("没有此用户");
          //返回信息，提示错误
           response.getWriter().write("<script> alert(\"没有此用户！\");parent.location.href='/onedata/login.html';</script>");
           return;
        }
        //
        response.sendRedirect("login.html");
        //log
		VisitControl.VisitLogModel visit = new VisitControl.VisitLogModel();
		visit.setReq_ip(IPAddressUtil.getClientIPAddress(request));
		visit.setReq_databaseid("0");
		visit.setReq_databasename("onedata_database");
		visit.setReq_interfacename("Login");
		visit.setReq_interfacetype("login");
		visit.setReq_project("onedata");
		visit.setReq_layer("userinfo");
		visit.setReq_keyword(username+","+password);
		visit.setResults(result);
		//visit.setToken(validation_code);
		VisitControl.log(visit);		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
	
	private String checkUser(String username,String password,HttpSession session)
	{
		String u = PropertiesUtil.getValue("AdminName");
		String p = PropertiesUtil.getValue("AdminPassword");
		
		if(u.equalsIgnoreCase(username))
		{
			//系统自带用户
			if(!u.equalsIgnoreCase(username))
			{
				return "hasNoUserName";
			}
			else
			{
				if(p.equalsIgnoreCase(password))
				{
					return "hasUserNameAndPasswordCorrect";
				}
				else
				{
					return "hasUserNameButPasswordInCorrect";
				}
			}
		}
		else
		{
			//数据库用户
			DataParam dataparam = new DataParam();
			dataparam.setCon(OneDataServer.SQLITEDBHandle.getEncodeConStr());
			dataparam.setLayer(OneDataServer.TablePrefix+"userinfo");
			dataparam.setOperation("query");
			dataparam.setQueryfields("user_name,phone");
			dataparam.setQueryrelations("or");
			dataparam.setQueryvalues(u+","+u);
			DataBaseDriver dataDriver = new DataBaseDriver();
			List<SuperObject> rs = dataDriver.query(dataparam);
			if(rs != null && rs.size()>0)
			{
				if(rs.get(0).getProperty("password").getString_value().equalsIgnoreCase(p))
				{
					session.setAttribute("userinfo", rs.get(0).getJSONString("json", null));
					return "hasUserNameAndPasswordCorrect";
				}
				else
				{
					return "hasUserNameButPasswordInCorrect";
				}
			}
			else
			{
				return "hasNoUserName";
			}
		}
		
	}

}
