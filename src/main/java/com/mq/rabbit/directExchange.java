package com.mq.rabbit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;

public class directExchange {

	/**
	 * 推送消息
	 */
	public static void publisher() throws IOException, TimeoutException {
		// 创建一个连接
		Connection conn = connectionFactoryUtil.GetRabbitConnection();
		// 创建通道
		Channel channel = conn.createChannel();
		// 声明队列【参数说明：参数一：队列名称，参数二：是否持久化；参数三：是否独占模式；参数四：消费者断开连接时是否删除队列；参数五：消息其他参数】
		channel.queueDeclare(config.QueueName, false, false, false, null);

		String message = String.format("当前时间：%s", new Date().getTime());
		// 发送内容【参数说明：参数一：交换机名称；参数二：队列名称，参数三：消息的其他属性-路由的headers信息；参数四：消息主体】
		channel.basicPublish("", config.QueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
		System.out.println("发送消息 => " + message);

		// 关闭连接
		channel.close();
		conn.close();
	}

	/**
	 * 消费消息
	 */
	public static void consumer(String workName) throws IOException, TimeoutException, InterruptedException {
		// 创建一个连接
		Connection conn = connectionFactoryUtil.GetRabbitConnection();
		// 创建通道
		Channel channel = conn.createChannel();
		// 声明队列【参数说明：参数一：队列名称，参数二：是否持久化；参数三：是否独占模式；参数四：消费者断开连接时是否删除队列；参数五：消息其他参数】
		channel.queueDeclare(config.QueueName, false, false, false, null);

		// 创建订阅器
		Consumer defaultConsumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				// String routingKey = envelope.getRoutingKey(); // 队列名称
				// String messageType = properties.getmessageType(); // 内容类型
				String message = new String(body, "utf-8"); // 消息正文
				System.out.println(Thread.currentThread().getName()+","+envelope.getDeliveryTag()+",["+workName + "]收到消息 => " + message);

				channel.basicAck(envelope.getDeliveryTag(), false); // 手动确认消息【参数说明：参数一：该消息的index；参数二：是否批量应答，true批量确认小于index的消息】
				
//				try {
//					Thread.sleep(5);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				} finally {
//					channel.basicAck(envelope.getDeliveryTag(), false); // 手动确认消息【参数说明：参数一：该消息的index；参数二：是否批量应答，true批量确认小于index的消息】
//				}

			}
		};

		// 接受消息
		channel.basicConsume(config.QueueName, false, "", defaultConsumer);

	}
	
	
	/**
	 * 批量消费和确认
	 * @param workName
	 * @throws IOException
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public static void consumerBatch(String workName) throws IOException, TimeoutException, InterruptedException {
		// 创建一个连接
		Connection conn = connectionFactoryUtil.GetRabbitConnection();
		// 创建通道
		Channel channel = conn.createChannel();
		// 声明队列【参数说明：参数一：队列名称，参数二：是否持久化；参数三：是否独占模式；参数四：消费者断开连接时是否删除队列；参数五：消息其他参数】
		channel.queueDeclare(config.QueueName, false, false, false, null);

		List<GetResponse> responseList = new ArrayList<>();
		
		long tag = 0,lastTag = 0;
		long i = 0;
		while (true) {
		    GetResponse getResponse = channel.basicGet(config.QueueName, false);
		    if (getResponse == null) {
		        break;
		    }
		    responseList.add(getResponse);
		    tag = getResponse.getEnvelope().getDeliveryTag();
		    i++;
		    if(i%config.BatchSize==0) {
		    	System.out.printf("Get responses :%s%n", responseList.size());
			    // handle messages
			    channel.basicAck(tag, true);
			    lastTag=tag;
		    }
		}
		
		if (tag==lastTag) {
			System.out.printf("完成消息处理，共收到消息："+responseList.size());
		} else {
			System.out.printf("Get responses :%s%n", responseList.size());
		    // handle messages
		    channel.basicAck(tag, true);
		}

	}
	

	/**
	 * 单个消费者
	 * 
	 * @throws IOException
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public static void singleConsumer() throws IOException, TimeoutException, InterruptedException {
		Connection conn = connectionFactoryUtil.GetRabbitConnection();
		Channel channel = conn.createChannel();
		channel.queueDeclare(config.QueueName, false, false, false, null);

		GetResponse resp = channel.basicGet(config.QueueName, false);
		String message = new String(resp.getBody(), "UTF-8");
		System.out.println("Single收到消息 => " + message);
		// // 消息拒绝
		// channel.basicReject(resp.getEnvelope().getDeliveryTag(), true);
		channel.basicAck(resp.getEnvelope().getDeliveryTag(), false); // 消息确认

	}

}
