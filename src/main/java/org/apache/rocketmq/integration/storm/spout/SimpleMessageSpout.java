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

import com.google.common.collect.MapMaker;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.integration.storm.MessagePushConsumer;
import org.apache.rocketmq.integration.storm.annotation.Extension;
import org.apache.rocketmq.integration.storm.domain.MessageStat;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Von Gosling
 */
@Extension("simple")
public class SimpleMessageSpout implements IRichSpout, MessageListenerConcurrently {
    private static final long serialVersionUID = -2277714452693486954L;

    private static final Logger LOG = LoggerFactory
        .getLogger(SimpleMessageSpout.class);

    private MessagePushConsumer consumer;

    private SpoutOutputCollector collector;
    private TopologyContext context;

    private BlockingQueue<Pair<MessageExt, MessageStat>> failureQueue = new LinkedBlockingQueue<Pair<MessageExt, MessageStat>>();
    private Map<String, Pair<MessageExt, MessageStat>> failureMsgs;

    private RocketMQConfig config;

    public void setConfig(RocketMQConfig config) {
        this.config = config;
    }

    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
        SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        this.failureMsgs = new MapMaker().makeMap();
        if (consumer == null) {
            try {
                config.setInstanceName(String.valueOf(context.getThisTaskId()));
                consumer = new MessagePushConsumer(config);

                consumer.start(this);
            } catch (Exception e) {
                LOG.error("Failed to init consumer !", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        if (!failureMsgs.isEmpty()) {
            for (Map.Entry<String, Pair<MessageExt, MessageStat>> entry : failureMsgs.entrySet()) {
                Pair<MessageExt, MessageStat> pair = entry.getValue();
                LOG.warn("Failed to handle message {},message statics {} !",
                    new Object[] {pair.getObject1(), pair.getObject2()});
            }
        }

        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public void activate() {
        consumer.resume();
    }

    public void deactivate() {
        consumer.suspend();
    }

    /**
     * Just handle failure message here
     *
     * @see org.apache.storm.spout.ISpout#nextTuple()
     */
    public void nextTuple() {
        Pair<MessageExt, MessageStat> pair = null;
        try {
            pair = failureQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        if (pair == null) {
            return;
        }

        pair.getObject2().setElapsedTime();
        collector.emit(new Values(pair.getObject1(), pair.getObject2()), pair.getObject1()
            .getMsgId());
    }

    public void ack(Object id) {
        String msgId = (String) id;

        failureMsgs.remove(msgId);
    }

    /**
     * if there are a lot of failure case, the performance will be bad because
     * consumer.viewMessage(msgId) isn't fast
     *
     * @see org.apache.storm.spout.ISpout#fail(Object)
     */
    public void fail(Object id) {
        handleFailure((String) id);
    }

    private void handleFailure(String msgId) {
        Pair<MessageExt, MessageStat> pair = failureMsgs.get(msgId);
        if (pair == null) {
            MessageExt msg;
            try {
                msg = consumer.getConsumer().viewMessage(msgId);
            } catch (Exception e) {
                LOG.error("Failed to get message {} from broker !", new Object[] {msgId}, e);
                return;
            }

            MessageStat stat = new MessageStat();

            pair = new Pair<MessageExt, MessageStat>(msg, stat);

            failureMsgs.put(msgId, pair);

            failureQueue.offer(pair);
            return;
        } else {
            int failureTime = pair.getObject2().getFailureTimes().incrementAndGet();
            if (config.getMaxFailTimes() < 0 || failureTime < config.getMaxFailTimes()) {
                failureQueue.offer(pair);
                return;
            } else {
                LOG.info("Failure too many times, skip message {} !", pair.getObject1());
                ack(msgId);
                return;
            }
        }
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
        ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                MessageStat msgStat = new MessageStat();
                collector.emit(new Values(msg, msgStat), msg.getMsgId());
            }
        } catch (Exception e) {
            LOG.error("Failed to emit message {} in context {},caused by {} !", msgs, this.context.getThisTaskId(), e.getCause());
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields("MessageExt", "MessageStat");
        declarer.declare(fields);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer.getConsumer();
    }

}
