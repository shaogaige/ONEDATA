package com.idata.server;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.idata.tool.PropertiesUtil;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet(description = "登录验证", urlPatterns = { "/LoginServlet" })
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServlet() {
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
        String validationCode = request.getParameter("validationCode");
        HttpSession session = request.getSession();
        String validation_code = (String)session.getAttribute("validation_code");
        
        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html;charset=utf-8;");
        if(!validationCode.equalsIgnoreCase(validation_code)){
        	//System.out.println("验证码错误");
            //返回信息
        	response.getWriter().write("<script> alert(\"验证码输入错误！\");parent.location.href='/SHAMSTOKEN/login.html';</script>");
        	return;
        }
        
        String result = checkUser(username,password);
        if (result.equals("hasUserNameAndPasswordCorrect")) {
             // System.out.println("用户名和密码均正确");
            //跳转到管理界面
            session.setAttribute("login", session.getId());
            response.sendRedirect("/ONEDATA/manager.html");
            return;
        } else if (result.equals("hasUserNameButPasswordInCorrect")) {
            //System.out.println("用户名正确,密码不正确");
            //返回信息，提示错误
            response.getWriter().write("<script> alert(\"用户名正确,密码不正确！\");parent.location.href='/SHAMSTOKEN/login.html';</script>");
            return;
        } else if (result.equals("hasNoUserName")) {
           // System.out.println("没有此用户");
          //返回信息，提示错误
           response.getWriter().write("<script> alert(\"没有此用户！\");parent.location.href='/SHAMSTOKEN/login.html';</script>");
           return;
        }
        //
        response.sendRedirect("login.html");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
	
	private String checkUser(String username,String password)
	{
		String u = PropertiesUtil.getValue("AdminName");
		String p = PropertiesUtil.getValue("AdminPassword");
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

}
