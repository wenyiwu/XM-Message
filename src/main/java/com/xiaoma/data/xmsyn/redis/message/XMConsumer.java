package com.xiaoma.data.xmsyn.redis.message;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class XMConsumer {
    private Jedis jedis;
    private String MSG_KEY;

    public XMConsumer(Jedis jedis, String topic) {
        this.jedis = jedis;
        MSG_KEY = topic;
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
            List<Object> msgList = read(jedis, num);
            if (msgList != null) {
                readMessage.onMessage(msgList);
            } else {
                waitForMessages();
            }
        }
    }

    private List<Object> read(Jedis jedis, int num){
        int canReadLine = jedis.llen(MSG_KEY).intValue();
        Pipeline pipeline = jedis.pipelined();
        if(canReadLine > num) {
            canReadLine = num;
        }
        if(canReadLine > 0) {
            for (int i = 0; i < canReadLine; i++) {
                pipeline.lpop(MSG_KEY);
            }
            List<Object> list = pipeline.syncAndReturnAll();

            return list;
        }
        return null;
    }
}
