/**
 * ClassName:DateCoder.java
 * Date:2018年6月26日
 */
package com.idata.tool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Creater:SHAO Gaige
 * Description:日期的编码器
 * Log:
 */
public class DateCoder implements ICoder {
	
	private static int YEAR = 1;
	private static int MONTH = 2;
	private static int DAY = 3;
	private static int HOUR = 4;
	private static int MINUTE = 5;
	private static int SECOND = 6;
	
	public static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	
	/**
	 * SimpleDateFormat("yyyyMMddHHmmss");
	 * @throws ParseException 
	 */
	public String code(String date)
	{
		Calendar now = null;
		if(date == null || "".equalsIgnoreCase(date))
		{
			now = Calendar.getInstance();
		}
		else
		{
			try 
			{
				Date _date =df.parse(date);
				now = Calendar.getInstance();
				now.setTime(_date);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		int year = now.get(Calendar.YEAR);
		int month = now.get(Calendar.MONTH) + 1;
		int day = now.get(Calendar.DAY_OF_MONTH);
		int hour = now.get(Calendar.HOUR_OF_DAY);
		int minute = now.get(Calendar.MINUTE);
		int second = now.get(Calendar.SECOND);
		
		//System.out.println(String.format("%04d", year)+"-"+String.format("%02d", month)+"-"+String.format("%02d", day)+" "+
		//String.format("%02d", hour)+":"+String.format("%02d", minute)+":"+String.format("%02d", second));
		//2017-8-28-22-6-42
		//2017-8-28-22-8-50
		String cipher = "";
		boolean flag = false;
		//day
		cipher += getEncodeString(day,DAY,flag);
		flag = isOdd(day);
		//month
		cipher += getEncodeString(month,MONTH,flag);
		flag = isOdd(month);
		//year
		cipher += getEncodeString(year,YEAR,flag);
		//flag = isOdd(year);
		//AD
		//cipher += "AD";
		//second
		cipher += getEncodeString(second,SECOND,false);
		flag = isOdd(second);
		//MM:HH or HH:MM
		if(flag)
		{
			cipher += getEncodeString(minute,MINUTE,true);
			cipher += getEncodeString(hour,HOUR,true);
		}
		else
		{
			cipher += getEncodeString(hour,HOUR,false);
			cipher += getEncodeString(minute,MINUTE,false);
		}
		
		//System.out.println(cipher);
		return cipher;
	}
	
	/**
	 * 解码
	 */
	public String uncode(String date)
	{
		char []dateArr = date.toCharArray();
		String plain = "";
		//plain += dateArr[8];
		//plain += dateArr[9];
		//plain +=" ";
		String year = "";
		String month = "";
		String day = "";
		String hour = "";
		String minute = "";
		String second = "";
		boolean flag = false;
		//day
		day += uncodeChar(dateArr[0]);
		int f = uncodeNum(Character.getNumericValue(dateArr[1]),DAY);
		day += f;
		flag = isOdd(f);
		//month
		if(flag)
		{
			month += uncodeNum(Character.getNumericValue(dateArr[3]),MONTH);
			f = uncodeChar(dateArr[2]);
			month += f;
			flag = isOdd(f);
		}
		else
		{
			month += uncodeChar(dateArr[2]);
			f = uncodeNum(Character.getNumericValue(dateArr[3]),MONTH);
			month += f;
			flag = isOdd(f);
		}
		//year
		if(flag)
		{
			year += uncodeNum(Character.getNumericValue(dateArr[7]),YEAR);
			year += uncodeChar(dateArr[6]);
			year += uncodeNum(Character.getNumericValue(dateArr[5]),YEAR);
			year += uncodeChar(dateArr[4]);
		}
		else
		{
			year += uncodeChar(dateArr[4]);
			year += uncodeNum(Character.getNumericValue(dateArr[5]),YEAR);
			year += uncodeChar(dateArr[6]);
			year += uncodeNum(Character.getNumericValue(dateArr[7]),YEAR);
		}
		//second
		second += uncodeChar(dateArr[8]);
		f = uncodeNum(Character.getNumericValue(dateArr[9]),SECOND);
		second += f;
		flag = isOdd(f);
		//minute:hour
		if(flag)
		{
			//minute
			minute += uncodeNum(Character.getNumericValue(dateArr[11]),MINUTE);
			minute += uncodeChar(dateArr[10]);
			//hour
			hour += uncodeNum(Character.getNumericValue(dateArr[13]),HOUR);
			hour += uncodeChar(dateArr[12]);
		}
		else
		{
			//hour
			hour += uncodeChar(dateArr[10]);
			hour += uncodeNum(Character.getNumericValue(dateArr[11]),HOUR);
			//minute
			minute += uncodeChar(dateArr[12]);
			minute += uncodeNum(Character.getNumericValue(dateArr[13]),MINUTE);
		}
		
		//System.out.println(year+"-"+month+"-"+day+" "+hour+":"+minute+":"+second);
		//plain = year+"-"+month+"-"+day+" "+hour+":"+minute+":"+second;
		plain = year+month+day+hour+minute+second;
		return plain;
	}
	
	public Date uncode2Date(String date)
	{
		String d = uncode(date);
		try 
		{
			Date _date = df.parse(d);
			return _date;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	private boolean isOdd(int num)
	{
		int f = num%2;
		if(f==0)
		{
			return false;
		}
		else
		{
			return true;
		}
	}
	
	private String getEncodeString(int num,int offset,boolean flag)
	{
		if(YEAR != offset)
		{
			int first = num/10;
		    int second = num%10;
		    if(flag)
		    {
		    	return getEncodeChar(second)+""+getEncodeNum(first,offset);
		    }
		    else
		    {
		    	return getEncodeChar(first)+""+getEncodeNum(second,offset);
		    }
		}
		else
		{
			int first = num/1000;
			int second = (num%1000)/100;
			int third = (num%100)/10;
			int fourth = num%10;
			
			if(flag)
			{
				return getEncodeChar(fourth)+""+getEncodeNum(third,offset)+""+getEncodeChar(second)+""+getEncodeNum(first,offset);
			}
			else
			{
				return getEncodeChar(first)+""+getEncodeNum(second,offset)+""+getEncodeChar(third)+""+getEncodeNum(fourth,offset);
			}
		}
		
		
	}
	
	private char getEncodeChar(int num)
	{
		return (char) (65+(9-num));//'A'+9-num
	}
	private int getEncodeNum(int num,int offset)
	{
		num += offset;
		return num%10;
	}
	private int uncodeChar(char c)
	{
		return 9-(c-65);
	}
	private int uncodeNum(int num,int offset)
	{
		if(num<offset)
    	{
    		num += 10;
    		return num-offset;
    	}
    	else
    	{
    		return num-offset;
    	} 	
	}

}
