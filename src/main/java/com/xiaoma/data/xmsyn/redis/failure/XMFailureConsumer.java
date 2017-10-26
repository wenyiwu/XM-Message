package com.xiaoma.data.xmsyn.redis.failure;

import com.xiaoma.data.xmsyn.redis.message.XMReadMessageImp;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class XMFailureConsumer {
    private JedisPool jedisPool;
    private String MSG_KEY;
    private String FAILURE_MAP_KEY;
    private Map<String, String> failureMap;
    private List<String> keyList;
    private AtomicInteger mapSize;
    private int mapI;
    private int currentCount;
    private ReentrantLock mapLock;

    public XMFailureConsumer(JedisPool jedisPool, String topic) {
        this.jedisPool = jedisPool;
        this.MSG_KEY = "topic:failure:" + topic + ":message:";
        this.FAILURE_MAP_KEY = "topic:failure:map:" + topic;
        mapSize = new AtomicInteger(0);
        mapI = 0;
        failureMap = getFailureMap();
        mapLock = new ReentrantLock();
    }

    private void waitForMessages() {
        try {
            // TODO el otro metodo podria hacer q no se consuman mensajes por un
            // tiempo si no llegan, de esta manera solo se esperan 500ms y se
            // controla que haya mensajes.
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }

    public void consume(boolean canRead, int num, XMReadMessageImp readMessage) {
        if(num > 0 && canRead) {
            List<Object> msgList = read(num);
            if (msgList != null) {
                readMessage.onMessage(msgList);
            } else {
                waitForMessages();
            }
        }
    }

    private List<Object> read(int num){
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();

        int canReadLine = mapI - currentCount;
        if(canReadLine > num) {
            canReadLine = num;
        }
        if(canReadLine > 0) {
            for (int i = 0; i < canReadLine; i++) {
                StringBuffer sb = new StringBuffer(MSG_KEY);
                sb.append(keyList.get(currentCount));
                currentCount++;
                pipeline.get(sb.toString());
            }
            List<Object> list = pipeline.syncAndReturnAll();
            jedis.close();
            return list;
        } else {
            Map map = getFailureMap();
            if(map != null) {
                failureMap = map;
            }
        }
        jedis.close();
        return null;
    }


    public Map<String, String> getFailureMap() {
        if(mapSize.get() == 0) {
            Jedis jedis = jedisPool.getResource();
            Map<String, String> map = jedis.hgetAll(FAILURE_MAP_KEY);
            if (map != null && map.size() > 0) {
                this.keyList = new ArrayList<>();
                keyList.addAll(map.keySet());
                mapI = keyList.size();
                mapSize.set(mapI);
                currentCount = 0;
                jedis.close();
                return map;
            }
            mapSize.set(0);
            currentCount = 0;
            jedis.close();
            return map;
        }
        return null;
    }

    public void delFailureMessage(String key) {
        Jedis jedis = jedisPool.getResource();
        jedis.del(getMessageKey(key));
        jedis.hdel(FAILURE_MAP_KEY, key);
        jedis.close();
        mapSize.decrementAndGet();
    }

    public void addFailureCount(String key, String value) {
        Jedis jedis = jedisPool.getResource();
        if(value == null) {
            return;
        }
        Integer v = new Integer(value);
        ++v;
        jedis.hset(FAILURE_MAP_KEY, key, v.toString());
        jedis.close();
        mapSize.decrementAndGet();
    }

    protected String getMessageKey(String count){
        String msgKey = MSG_KEY  + count;
        return msgKey;
    }

    public String getFailureCountForKey(String key) {
        return failureMap.get(key);
    }
}
