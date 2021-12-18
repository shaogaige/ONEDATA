/**
 * ClassName:QRUtil.java
 * Date:2020年7月18日
 */
package com.idata.tool;

import java.io.ByteArrayOutputStream;
import com.idata.tool.PropertiesUtil;
import com.qrcode.wrapper.QrCodeGenWrapper;
import com.qrcode.wrapper.QrCodeGenWrapper.Builder;
import com.qrcode.wrapper.QrCodeOptions;

/**
 * Creater:SHAO Gaige
 * Description:QR图片生成处理类
 * Log:
 */
public class QRUtil {
	
	public static byte[] getQRimage(String url,String fromat)
	{
		try 
		{
			Builder builder = QrCodeGenWrapper.of(url).setPicType(fromat);
	        //中间标识logo
			String logo = "";
			if(PropertiesUtil.existFile(PropertiesUtil.getValue("_systempath_"), "logo.png"))
			{
				logo = PropertiesUtil.getValue("_systempath_")+"//"+"logo.png";
				logo = logo.replaceAll("//", "/");
				builder.setLogo(logo)
			    .setLogoStyle(QrCodeOptions.LogoStyle.ROUND)
			    .setLogoBgColor(0xfffefefe)
			    .setLogoBorderBgColor(0xffc7c7c7)
			    .setLogoBorder(true);
			}
			//根据ID获取背景图片
			//String bg = "";
			/*RoadLabelControl rc = new RoadLabelControl();
			List<SuperObject> os = rc.query("id", id, 1, 1);
			if(os != null && os.size()>0)
			{
				SuperObject o = os.get(0);
				Value v = o.getProperty("backcolor");
				if(v != null)
				{
					String bc = v.getString_value();
					if("".equalsIgnoreCase(bc))
					{
						bg = "bg1.png";
					}
					else if("".equalsIgnoreCase(bc))
					{
						bg = "bg2.png";
					}
					if(PropertiesUtil.existFile(PropertiesUtil.configPath, bg))
					{
						bg = PropertiesUtil.configPath+"//"+bg;
						bg = bg.replaceAll("//", "/");
						builder.setBgImg(bg)
				        .setBgStyle(QrCodeOptions.BgImgStyle.PENETRATE)
				        .setBgW(Server.QRWidth)
				        .setBgH(Server.QRHeight);
					}
				}
			}*/
			
			ByteArrayOutputStream pic = builder
			        .setW(Integer.valueOf(PropertiesUtil.getValue("QRSIZE_W")))
			        .setH(Integer.valueOf(PropertiesUtil.getValue("QRSIZE_H")))
			        .asStream();
			return pic.toByteArray();
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "二维码生成失败，请联系管理员".getBytes();
		} 
		
	}

}
