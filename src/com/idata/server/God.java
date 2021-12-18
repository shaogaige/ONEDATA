package com.idata.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.tomcat.util.http.fileupload.FileItem;
import org.apache.tomcat.util.http.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.http.fileupload.servlet.ServletFileUpload;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;

import com.idata.core.OneDataServer;
import com.idata.cotrol.FunctionControl;
import com.idata.cotrol.TokenControl;
import com.idata.cotrol.VisitControl;
import com.idata.tool.IPAddressUtil;
import com.idata.tool.LogUtil;

/**
 * Servlet implementation class God
 */
@WebServlet(description = "类函数接口", urlPatterns = { "/God" })
public class God extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public God() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		//项目名称
		String project = request.getParameter("project");
		//访问权限
		String token = request.getParameter("token");
		if(OneDataServer.checkToken)
		{
			TokenControl tokencontrol = new TokenControl();
			boolean f = tokencontrol.check(token);
			if(!f)
			{
				//设置头
				response.setCharacterEncoding("UTF-8");
				response.setContentType("application/json;charset=utf-8");
				response.setHeader("Access-Control-Allow-Origin", "*");
				//参数有错误
				response.getWriter().write("{\"state\":false,\"message\":\"非法访问用户,token不存在或已过期,请联系管理员！\",\"size\":0,\"data\":[]}");
				return;
			}
		}
		//类名称
		String classname = request.getParameter("class");
		//函数名称
		String fun = request.getParameter("fun");
		//参数
		String params = request.getParameter("params");
		if(params != null) {
			params = new String(params.getBytes("ISO8859-1"),"UTF-8");
			params = URLDecoder.decode(params,"UTF-8");
		}
		if(params == null || "".equalsIgnoreCase(params))
		{
			boolean multipart = ServletFileUpload.isMultipartContent(request);
			if(multipart)
			{
				// 4、使用ServletFileUpload解析器解析上传数据，解析结果返回的是一个List<FileItem>集合
				try 
				{
					// 使用Apache文件上传组件处理文件上传步骤：
					// 1、创建一个DiskFileItemFactory工厂
					DiskFileItemFactory factory = new DiskFileItemFactory();
					// 2、创建一个文件上传解析器
					ServletFileUpload upload = new ServletFileUpload(factory);
					// 解决上传文件名的中文乱码
					upload.setHeaderEncoding("UTF-8");
					List<FileItem> list = upload.parseRequest(new ServletRequestContext(request));
					for (FileItem item : list)
					{
						// 如果fileitem中封装的是普通输入项的数据
						if (item.isFormField())
						{
							//String name = item.getFieldName();
							// 解决普通输入项的数据的中文乱码问题
							String value = item.getString("UTF-8");
							value = new String(value.getBytes("ISO8859-1"), "UTF-8");
							value = URLDecoder.decode(value, "UTF-8");
							//System.out.println("jsondata3:"+value);
							params = value;
						}
				     }
				}catch (Exception e) {
					// TODO Auto-generated catch block
					LogUtil.error(e);
				}
			}
		}
		
		String result = "";
		if(classname == null || "".equalsIgnoreCase(classname))
		{
			result = "{\"state\":false,\"message\":\"class参数不能为空！\",\"size\":0,\"data\":[]}";
		}
		else
		{
			if(fun == null || "".equalsIgnoreCase(fun))
			{
				result = "{\"state\":false,\"message\":\"fun参数不能为空！\",\"size\":0,\"data\":[]}";
			}
			else
			{
				if(params == null || "".equalsIgnoreCase(params))
				{
					result = "{\"state\":false,\"message\":\"params参数不能为空！\",\"size\":0,\"data\":[]}";
				}
				else
				{
					FunctionControl func = new FunctionControl();
					result = func.process(classname, fun, params);
				}
			}
		}
		
		//结果输出
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// 创建 GZIPOutputStream 对象
        GZIPOutputStream gzipOut = new GZIPOutputStream(bout);
        // 将响应的数据写到 Gzip 压缩流中
        gzipOut.write(result.getBytes("UTF-8")); 
        gzipOut.close(); // 将数据刷新到  bout 字节流数组
		
		//设置头
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json;charset=utf-8");
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE");  
        response.setHeader("Access-Control-Max-Age", "3600");  
        response.setHeader("Access-Control-Allow-Headers", "x-requested-with");
		
		//response.getWriter().write(result);
		//设置GZIP压缩
		response.setHeader("Content-Encoding", "gzip");
		
		response.getOutputStream().write(bout.toByteArray());
		response.getOutputStream().flush();
		response.getOutputStream().close();
		//end	
		
		if(OneDataServer.visitlog)
		{
			//log
			VisitControl.VisitLogModel visit = new VisitControl.VisitLogModel();
			visit.setReq_ip(IPAddressUtil.getClientIPAddress(request));
			visit.setReq_databaseid("0");
			visit.setReq_databasename("onedata_database");
			visit.setReq_interfacename("God");
			visit.setReq_interfacetype("query");
			visit.setReq_project(project);
			visit.setReq_layer(classname+"."+fun);
			visit.setReq_keyword(request.getQueryString());
			visit.setResults(result);
			visit.setToken(token);
			VisitControl.log(visit);
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
