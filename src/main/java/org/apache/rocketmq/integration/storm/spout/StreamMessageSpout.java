/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.integration.storm.spout;

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.integration.storm.annotation.Extension;
import org.apache.rocketmq.integration.storm.domain.BatchMessage;
import org.apache.rocketmq.integration.storm.domain.MessageCacheItem;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Von Gosling
 */
@Extension("stream")
public class StreamMessageSpout extends BatchMessageSpout {
    private static final long serialVersionUID = 464153253576782163L;

    private static final Logger LOG = LoggerFactory
        .getLogger(StreamMessageSpout.class);

    private final Queue<MessageCacheItem> msgQueue = new ConcurrentLinkedQueue<MessageCacheItem>();
    private RotatingMap<String, MessageCacheItem> msgCache;

    /**
     * This field is used to check whether one batch is finish or not
     */
    private Map<UUID, BatchMsgsTag> batchMsgsMap = new ConcurrentHashMap<UUID, BatchMsgsTag>();

    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
        final SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        RotatingMap.ExpiredCallback<String, MessageCacheItem> callback = new RotatingMap.ExpiredCallback<String, MessageCacheItem>() {
            public void expire(String key, MessageCacheItem val) {
                LOG.warn("Long time no ack,key is {},value is {} !", key, val);
                msgCache.put(key, val);
                fail(key);
            }

        };
        msgCache = new RotatingMap<String, MessageCacheItem>(3600 * 5, callback);

        LOG.info("Topology {} opened {} spout successfully !",
            new Object[] {topologyName, config.getTopic()});
    }

    public void prepareMsg() {
        while (true) {
            BatchMessage msgTuple = null;
            try {
                msgTuple = super.getBatchQueue().poll(1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return;
            }
            if (msgTuple == null) {
                return;
            }

            if (msgTuple.getMsgList().size() == 0) {
                super.finish(msgTuple.getBatchId());
                return;
            }

            BatchMsgsTag partTag = new BatchMsgsTag();
            Set<String> msgIds = partTag.getMsgIds();
            for (MessageExt msg : msgTuple.getMsgList()) {
                String msgId = msg.getMsgId();
                msgIds.add(msgId);
                MessageCacheItem item = new MessageCacheItem(msgTuple.getBatchId(), msg,
                    msgTuple.getMessageStat());
                msgCache.put(msgId, item);
                msgQueue.offer(item);
            }
            batchMsgsMap.put(msgTuple.getBatchId(), partTag);
        }
    }

    @Override
    public void nextTuple() {
        MessageCacheItem cacheItem = msgQueue.poll();
        if (cacheItem != null) {
            Values values = new Values(cacheItem.getMsg(), cacheItem.getMsgStat());
            String messageId = cacheItem.getMsg().getMsgId();
            collector.emit(values, messageId);

            LOG.debug("Emited tuple {},mssageId is {} !", values, messageId);
            return;
        }

        prepareMsg();
    }

    public void finish(String msgId) {
        MessageCacheItem cacheItem = (MessageCacheItem) msgCache.remove(msgId);
        if (cacheItem == null) {
            LOG.warn("Failed to get from cache {} !", msgId);
            return;
        }

        UUID batchId = cacheItem.getId();
        BatchMsgsTag partTag = batchMsgsMap.get(batchId);
        if (partTag == null) {
            throw new RuntimeException("In partOffset map, no entry of " + batchId);
        }

        Set<String> msgIds = partTag.getMsgIds();

        msgIds.remove(msgId);

        if (msgIds.size() == 0) {
            batchMsgsMap.remove(batchId);
            super.finish(batchId);
        }

    }

    @Override
    public void ack(final Object id) {
        if (id instanceof String) {
            finish((String) id);
        } else {
            LOG.error("Id isn't Long, type is {} !", id.getClass().getName());
        }
    }

    public void handleFail(String msgId) {
        MessageCacheItem cacheItem = msgCache.get(msgId);
        if (cacheItem == null) {
            LOG.warn("Failed to get cached values {} !", msgId);
            return;
        }

        LOG.info("Failed to handle {} !", cacheItem);

        int failTime = cacheItem.getMsgStat().getFailureTimes().incrementAndGet();
        if (config.getMaxFailTimes() < 0 || failTime < config.getMaxFailTimes()) {
            msgQueue.offer(cacheItem);
        } else {
            LOG.info("Skip message {} !", cacheItem.getMsg().toString());
            finish(msgId);
        }

    }

    @Override
    public void fail(final Object id) {
        if (id instanceof String) {
            handleFail((String) id);
        } else {
            LOG.error("Id isn't Long, type is {}", id.getClass().getName());
        }
    }

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MessageExt"));
    }

    public static class BatchMsgsTag {
        private final Set<String> msgIds;
        private final long createTs;

        public BatchMsgsTag() {
            this.msgIds = Sets.newHashSet();
            this.createTs = System.currentTimeMillis();
        }

        public Set<String> getMsgIds() {
            return msgIds;
        }

        public long getCreateTs() {
            return createTs;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

}
