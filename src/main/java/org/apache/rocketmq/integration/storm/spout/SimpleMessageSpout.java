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

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.Pair;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.google.common.collect.MapMaker;

@Extension("simple")
public class SimpleMessageSpout implements IRichSpout, MessageListenerConcurrently {
	
    private static final long serialVersionUID = -2277714452693486954L;

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMessageSpout.class);
    
    private MessagePushConsumer consumer;

    private SpoutOutputCollector collector;
    
    private TopologyContext context;

    // 失败阻塞队列
    private BlockingQueue<Pair<MessageExt, MessageStat>> failureQueue = new LinkedBlockingQueue<Pair<MessageExt, MessageStat>>();
    
    private Map<String, Pair<MessageExt, MessageStat>> failureMsgs;

    private RocketMQConfig config;

    public void setConfig(RocketMQConfig config) {
        this.config = config;
    }

    /**
     * 初始化时，只执行一次
     * 初始化消费者
     */
    @SuppressWarnings("rawtypes") 
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        // Builds a thread-safe map
        this.failureMsgs = new MapMaker().makeMap();
        // 初始化消费者
        if (consumer == null) {
            try {
            	// Unique mark for every JVM instance，Gets the task id of this task.
                config.setInstanceName(String.valueOf(context.getThisTaskId()));
                consumer = new MessagePushConsumer(config);
                consumer.start(this);
            } catch (Exception e) {
                LOG.error("Failed to init consumer !", e);
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * Called when an ISpout is going to be shutdown. There is no guarentee that close
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     *
     * The one context where close is guaranteed to be called is a topology is
     * killed when running Storm in local mode.
     */
    public void close() {
    	// 如果存在失败消息，记录失败消息
        if (!failureMsgs.isEmpty()) {
            for (Map.Entry<String, Pair<MessageExt, MessageStat>> entry : failureMsgs.entrySet()) {
                Pair<MessageExt, MessageStat> pair = entry.getValue();
                LOG.warn("Failed to handle message {},message statics {} !",
                    new Object[] {pair.getObject1(), pair.getObject2()});
            }
        }
        
        // 关闭消费者进程
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    /**
     * 状态变为活动时触发，恢复消费者进程
     */
    public void activate() {
        consumer.resume();
    }
    /**
     * 状态变为非活动时触发，暂停消费者进程
     */
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
        collector.emit(new Values(pair.getObject1(), pair.getObject2()), pair.getObject1().getMsgId());
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
    
    /**
     * 消费消息
     */
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
	        for (MessageExt msg : msgs) {
	            MessageStat msgStat = new MessageStat();
	            System.out.println("========******============"+ byteArrayToStr(msg.getBody()));
	            collector.emit(new Values(msg, msgStat), msg.getMsgId());
	        }
        } catch (Exception e) {
            LOG.error("Failed to emit message {} in context {},caused by {} !", msgs, this.context.getThisTaskId(), e.getCause());
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
    
    public static String byteArrayToStr(byte[] byteArray) {
        if (byteArray == null) {
            return null;
        }
        String str = new String(byteArray);
        return str;
    }

    /**
     * Declare the output schema for all the streams of this topology.
     */
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
