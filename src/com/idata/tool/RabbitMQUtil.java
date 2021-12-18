/**
 * ClassName:RabbitMQUtil.java
 * Date:2020年6月24日
 */
package com.idata.tool;

import java.io.IOException;

import com.idata.cotrol.VisitControl;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * Creater:SHAO Gaige
 * Description:RabbitMQ工具类
 * Log:支持实现消息异步处理
 */
public class RabbitMQUtil {
	
	private static Connection connection;
	
	private static Channel channel;
	
	static 
	{
		doShutDownWork();
	}
	
	public static void initConsumer() throws Exception
	{
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.getValue("RMQHOST"));
        factory.setPort(Integer.valueOf(PropertiesUtil.getValue("RMQPORT")));
        factory.setUsername(PropertiesUtil.getValue("RMQUSER"));
        factory.setPassword(PropertiesUtil.getValue("RMQPASSWORD"));
        //factory.setVirtualHost("vhostOne");

        Connection connection =  factory.newConnection();

        Channel channel = connection.createChannel();
        String queueName = PropertiesUtil.getValue("RMQQUEUENAME");
        channel.queueDeclare(queueName,true,false,false,null);

        channel.basicQos(10);  //每次取10条消息

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                //消费消费
                String msg = new String(body,"utf-8");
                //System.out.println("consume msg: "+msg);
                try 
                {
                	VisitControl.save(msg);
                    //TimeUnit.MILLISECONDS.sleep((long) (Math.random()*500));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //手动消息确认
                getChannel().basicAck(envelope.getDeliveryTag(),false);
            }
        };
        //调用消费消息
        channel.basicConsume(queueName,false,queueName,consumer);
	}
	
	public static void initProducer() throws Exception
	{
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.getValue("RMQHOST"));
        factory.setPort(Integer.valueOf(PropertiesUtil.getValue("RMQPORT")));
        factory.setUsername(PropertiesUtil.getValue("RMQUSER"));
        factory.setPassword(PropertiesUtil.getValue("RMQPASSWORD"));
        //factory.setVirtualHost("vhostOne");

        connection =  factory.newConnection();

        channel = connection.createChannel();
        String queueName = PropertiesUtil.getValue("RMQQUEUENAME");
        String exchangeName = PropertiesUtil.getValue("RMQEXNAME");
        String routingKey = PropertiesUtil.getValue("RMQROUTKEY");
        channel.exchangeDeclare(exchangeName,"direct");
        channel.queueDeclare(queueName,true,false,false,null);
        channel.queueBind(queueName,exchangeName,routingKey);
	}
	
	
	private static void doShutDownWork() 
	{
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
		     try 
		     {
		    	 if(channel != null)
		    	 {
		    		 channel.close();
		    	 }
		    	 if(connection != null)
		    	 {
		    		 connection.close();
		    	 }
		     } 
		     catch (Exception ex) 
		     {
		    	 ex.printStackTrace();
		     }    
		    }  
		   });
	}
	
	public static boolean sendMessage(String msg) throws Exception
	{
		 //msg = "msg"+Math.random()*100;
		 if(channel == null)
		 {
			 return false;
		 }
         channel.basicPublish("exchangerOne","queueOne",null,msg.getBytes());  //发送消息
         //System.out.println("produce msg :"+msg);
         return true;
	}
	
	
	
}
