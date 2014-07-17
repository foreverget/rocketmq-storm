package com.alibaba.storm.mq;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.google.common.collect.MapMaker;

/**
 * @author Von Gosling
 */
public class RandomAsyncCommit {
    private final ConcurrentMap<MessageQueue, CachedQueue> mqCachedTable = new MapMaker().makeMap();

    public void putMessages(final MessageQueue mq, final List<MessageExt> msgs) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null == cachedQueue) {
            cachedQueue = new CachedQueue();
            this.mqCachedTable.put(mq, cachedQueue);
        }
        for (MessageExt msg : msgs) {
            cachedQueue.getMsgCachedTable().put(msg.getQueueOffset(), msg);
        }
    }

    public void removeMessage(final MessageQueue mq, long offset) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null != cachedQueue) {
            cachedQueue.getMsgCachedTable().remove(offset);
        }
    }

    public long commitableOffset(final MessageQueue mq) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null != cachedQueue) {
            return cachedQueue.getMsgCachedTable().firstKey();
        }

        return -1;
    }
}
