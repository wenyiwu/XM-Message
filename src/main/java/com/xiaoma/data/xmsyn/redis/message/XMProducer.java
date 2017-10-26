package com.xiaoma.data.xmsyn.redis.message;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.concurrent.locks.ReentrantLock;

public class XMProducer implements Runnable {
    protected Jedis jedis;
    protected String MSG_KEY;
    protected int pipCount = 1;
    protected ReentrantLock lock;
    protected Pipeline pipeline;

    public XMProducer(Jedis jedis, String MSG_KEY) {
        this.jedis = jedis;
        this.MSG_KEY = MSG_KEY;
        lock = new ReentrantLock();
        pipeline = jedis.pipelined();
    }

    public void publish(String message) {

        lock.lock();
        pipCount++;
        pipeline.rpush(MSG_KEY, message);

        if(pipCount % 100 == 0){
            pipeline.sync();
        }
        lock.unlock();
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
