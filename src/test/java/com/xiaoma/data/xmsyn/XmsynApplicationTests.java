package com.xiaoma.data.xmsyn;

import com.sun.org.apache.xpath.internal.SourceTree;
import com.xiaoma.data.xmsyn.redis.failure.MsgFailureInfo;
import com.xiaoma.data.xmsyn.redis.failure.XMFailureConsumer;
import com.xiaoma.data.xmsyn.redis.failure.XMFailureProducer;
import com.xiaoma.data.xmsyn.redis.message.XMConsumer;
import com.xiaoma.data.xmsyn.redis.message.XMProducer;
import com.xiaoma.data.xmsyn.redis.message.XMReadMessageImp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class XmsynApplicationTests {

	@Autowired
	JedisPool jedisPool;

	private final static String MAX_FAILURE_COUNT = "5";

	@Test
	public void contextLoads() {
	}


	@Test
	public void testMessageProducer() {
		XMProducer producer = new XMProducer(jedisPool.getResource(), "test");
		System.out.println(System.currentTimeMillis());
		for(int i = 0; i <= 1000; i++){
			producer.publish("message: " + i);
		}
		System.out.println(System.currentTimeMillis());
	}

	@Test
	public void testMessageConsumer() {
		XMConsumer consumer = new XMConsumer(jedisPool.getResource(), "test");
		while(true) {
			consumer.consume(true, 1000, new XMReadMessageImp() {
				@Override
				public void onMessage(List<Object> message) {
					for (Object str : message) {
						System.out.println((String) str);
					}
				}
			});
		}
	}

	@Test
	public void testFailureMessageProducer() {
		XMFailureProducer producer = new XMFailureProducer(jedisPool.getResource(), "test");
		for(int i = 0; i <= 1000; i++){
			producer.publish("failure:message: " + i);
		}
	}

	@Test
	public void testFailureMessageConsumer() {
		XMFailureConsumer consumer = new XMFailureConsumer(jedisPool, "test");
		while(true) {
			consumer.consume(true, 1000, new XMReadMessageImp() {
				@Override
				public void onMessage(List<Object> message) {
					for(Object object:message) {
						MsgFailureInfo msgFailureInfo = (MsgFailureInfo)object;
						String key = msgFailureInfo.getKey();
						if(MAX_FAILURE_COUNT.equals(msgFailureInfo.getKey())) {
							//you can do something in there.
							consumer.delFailureMessage(key);
							System.out.println("The number of failures has reached its maximum");
						}else {
							//add the failure count
							consumer.addFailureCount(key, consumer.getFailureCountForKey(key));
						}
					}
				}
			});
		}
	}
}
