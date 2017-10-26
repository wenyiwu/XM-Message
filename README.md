#XM-Message
利用redis做消息队列，并包含失败重发机制。

此消息队列使用的都是批量读/写入redis。

亲测，redis以一条消息为单位进行存取，效率为`2000/s`

100条为单位，效率为`10W/s`


```javascript
        pipeline.rpush(MSG_KEY, message);

        if(pipCount % 100 == 0){
            pipeline.sync();
        }
```
1.生产者使用方式：
    	XMProducer producer = new XMProducer(jedisPool.getResource(), "test");
		System.out.println(System.currentTimeMillis());

2.消费者使用方式
```javascript
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
```


3.消息失败存入失败队列
```javascript

		XMFailureProducer producer = new XMFailureProducer(jedisPool.getResource(), "test");
			producer.publish("failure:message: " + i);
```

4.从失败队列中读取重试
```javascript
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
```