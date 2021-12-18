package com.idata.server;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
//import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.idata.core.OneDataServer;

/**
 * Servlet Filter implementation class SessionFilter
 * @WebFilter(description = "过滤器", urlPatterns = { "/SessionFilter" })
 */

public class SessionFilter implements Filter {

    /**
     * Default constructor. 
     */
    public SessionFilter() {
        // TODO Auto-generated constructor stub
    	//System.out.println("filter...");
    }

	/**
	 * @see Filter#destroy()
	 */
	public void destroy() {
		// TODO Auto-generated method stub
	}

	/**
	 * @see Filter#doFilter(ServletRequest, ServletResponse, FilterChain)
	 */
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		// TODO Auto-generated method stub
		// place your code here
		HttpServletRequest req = (HttpServletRequest) request;
	    HttpServletResponse res = (HttpServletResponse) response;
	    HttpSession session = req.getSession();
	    //System.out.println("filter use...");
	    if(OneDataServer.doFilter)
	    {
	    	//判断session是否过期
		    if (session.getAttribute("sid") == null) {
		      String errors = "您还没有登录，或者session已过期。请先登陆!";
		      request.setAttribute("Message", errors);
		      session.setAttribute("eid", session.getId());
		      //设置头
		      res.setCharacterEncoding("UTF-8");
		      res.setContentType("application/json;charset=utf-8");
			  res.setHeader("Access-Control-Allow-Origin", "*");
			  //参数有错误
			  res.getWriter().write("{\"state\":false,\"message\":\"您还没有登录，或者session已过期。请先登陆!\",\"size\":0,\"data\":[]}");
		      //跳转至登录页面
		      //res.sendRedirect("/onedata/login.html");
		      //request.getRequestDispatcher("/login.html").forward(request, response);
		    }
	    }
	    else
	    {
	    	// pass the request along the filter chain
	    	chain.doFilter(request, response);
	    }
	}

	/**
	 * @see Filter#init(FilterConfig)
	 */
	public void init(FilterConfig fConfig) throws ServletException {
		// TODO Auto-generated method stub
		//System.out.println("filter init...");
	}

}
