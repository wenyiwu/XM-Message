package com.xiaoma.data.xmsyn.redis.failure;

public class MsgFailureInfo {
    private Object msgInfo;
    private String key;

    public Object getMsgInfo() {
        return msgInfo;
    }

    public void setMsgInfo(Object msgInfo) {
        this.msgInfo = msgInfo;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
