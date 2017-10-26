package com.xiaoma.data.xmsyn.redis.message;

import java.util.List;

public interface XMReadMessageImp {
    void onMessage(List<Object> message);
}
