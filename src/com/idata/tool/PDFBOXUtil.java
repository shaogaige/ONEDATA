/**
 * ClassName:PDFBOXUtil.java
 * Date:2018年12月11日
 */
package com.idata.tool;

import java.io.File;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

/**
 * Creater:SHAO Gaige
 * Description:PDF文件读取工具类
 * Log:
 */
public class PDFBOXUtil {
	
	/**
	 * 读取pdf文件内容
	 * @param filePath
	 * @return String
	 */
	public static String readPDF(String filePath)
	{
		//创建文档对象
        PDDocument doc =null;
        String content="";
        try {
            //加载一个pdf对象
            doc = PDDocument.load(new File(filePath));
            //获取一个PDFTextStripper文本剥离对象  
            PDFTextStripper textStripper = new PDFTextStripper();
            content = textStripper.getText(doc);
            System.out.println("读取pdf文件页数："+doc.getNumberOfPages());
            //关闭文档
            doc.close();
            return content;
        } catch (Exception e) {
            // TODO: handle exception
        	System.out.println("读取pdf文件出现异常：");
        	e.printStackTrace();
        	return "";
        }
	}

}
