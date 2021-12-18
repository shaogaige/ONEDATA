/**
 * ClassName:ExcelOutput.java
 * Date:2021年1月11日
 */
package com.idata.tool;

import java.io.File;
import java.io.IOException;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableImage;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import jxl.write.biff.RowsExceededException;

/**
 * Creater:SHAO Gaige
 * Description:
 * Log:
 */
public class ExcelOutput {
	
private WritableWorkbook excelFile = null;
	
	private WritableSheet sheet = null;
	
	/**
	 * 
	 * @param filePath
	 */
	public ExcelOutput(String filePath)
	{
		if(filePath != null && !"".equals(filePath))
		{
			try 
			{
				this.excelFile = Workbook.createWorkbook(new File(filePath));
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("EXCEl Create Fail !! File Path:"+filePath);
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 
	 * @param sheetName
	 * @return
	 */
	public boolean createSheet(String sheetName,int sheetNum)
	{
		try
		{
			this.sheet = this.excelFile.createSheet(sheetName, sheetNum);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	public Label addLabel(int row,int col ,String value)
	{
		Label label = new Label(col,row,value);
		try {
			
			this.sheet.addCell(label);
			
		} catch (RowsExceededException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (WriteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return label;
	}
	
	public void addImage(int row,int col,byte[] img,int height)
	{
		try
		{
			WritableImage image = new WritableImage(col,row,1,1,img);
			this.sheet.setRowView(row, height, false); //设置行高
			this.sheet.addImage(image);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return;
		}
	}
	
	public void saveLabels()
	{
		try 
		{
			this.excelFile.write();
			this.excelFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WriteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
