package com.xiaoma.data.xmsyn.redis.failure;

import com.alibaba.fastjson.JSON;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.concurrent.locks.ReentrantLock;


public class XMFailureProducer implements Runnable {
    private String FAILURE_MAP_KEY;
    private Jedis jedis;
    private String MSG_KEY;
    private String COUNT_KEY;
    private Long msgCount;
    private int pipCount = 1;
    private ReentrantLock lock;
    private Pipeline pipeline;
    private final static int THE_MAX_READ_COUNT = 1000;

    public XMFailureProducer(Jedis jedis, String MSG_KEY) {
        this.jedis = jedis;
        this.COUNT_KEY = "failure:" + MSG_KEY;
        msgCount = new Long(-1);
        lock = new ReentrantLock();
        pipeline = jedis.pipelined();
        this.FAILURE_MAP_KEY = "failure:map:" + MSG_KEY;
    }

    public void publish(Object msgPushInfo) {
        publish(msgPushInfo, 0);
    }

    public void publish(Object msgPushInfo, int seconds) {
        MsgFailureInfo msgFailureInfo = new MsgFailureInfo();
        msgFailureInfo.setMsgInfo(msgPushInfo);
        if(msgCount.intValue() == -1){
            msgCount = getNextMessageId().longValue();

        }else {
            ++msgCount;
            ++pipCount;
        }
        lock.lock();
        pipeline.incr(COUNT_KEY);

        String msgKey = getMessageKey(msgCount);
        msgFailureInfo.setKey(msgCount.toString());
        pipeline.set(msgKey, JSON.toJSONString(msgFailureInfo));
        pipeline.hset(FAILURE_MAP_KEY, msgCount.toString(), "1");
        if(seconds > 0){
            pipeline.expire(msgKey, seconds);
        }
        if(pipCount % THE_MAX_READ_COUNT == 0){
            pipeline.sync();
            System.out.println(pipCount);
        }
        lock.unlock();
    }

    protected Integer getNextMessageId() {
        final String slastMessageId = jedis.get(COUNT_KEY);
        Integer lastMessageId = 0;
        if (slastMessageId != null) {
            lastMessageId = Integer.parseInt(slastMessageId);
        }
        if(lastMessageId.equals(0)){
            jedis.set(COUNT_KEY, lastMessageId.toString());
        }
        return lastMessageId;
    }

    protected String getMessageKey(Long count){
        String msgKey = MSG_KEY + ":" + count.toString();
        return msgKey;
    }

    @Override
    public void run() {
        while(true) {
            lock.lock();
            final long timeInterval = 500;
            pipeline.sync();
            pipCount = 1;
            lock.unlock();
            try {
                Thread.sleep(timeInterval);
            } catch (InterruptedException e) {
                //lock.unlock();
                e.printStackTrace();
            }
        }
    }
}
