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

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.integration.storm.MessagePullConsumer;
import org.apache.rocketmq.integration.storm.domain.BatchMessage;
import org.apache.rocketmq.integration.storm.domain.QueueOffsetCache;
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
 * @author von gosling
 */
public class SimplePullMessageSpout implements IRichSpout {
    private static final long serialVersionUID = -5561450001033205169L;

    private static final Logger LOG = LoggerFactory
        .getLogger(SimplePullMessageSpout.class);

    private MessagePullConsumer consumer;

    protected SpoutOutputCollector collector;
    protected TopologyContext context;

    private RocketMQConfig config;

    protected Map<UUID, BatchMessage> batchCache = new MapMaker().makeMap();

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
        SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        if (consumer == null) {
            try {
                config.setInstanceName(String.valueOf(context.getThisTaskId()));
                consumer = new MessagePullConsumer(config);
                consumer.start();

                Set<MessageQueue> mqs = consumer.getConsumer().fetchSubscribeMessageQueues(
                    config.getTopic());

                consumer.getTopicQueueMappings().put(config.getTopic(), Lists.newArrayList(mqs));
            } catch (Exception e) {
                LOG.error("Error occurred !", e);
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void close() {
        consumer.shutdown();
    }

    @Override
    public void activate() {
        consumer.resume();
    }

    @Override
    public void deactivate() {
        consumer.suspend();
    }

    @Override
    public void nextTuple() {
        handleAndEmit();
    }

    private void handleAndEmit() {
        List<MessageQueue> mqs = consumer.getTopicQueueMappings().get(config.getTopic());
        for (MessageQueue mq : mqs) {
            try {
                PullResult pullResult = consumer.getConsumer().pullBlockIfNotFound(mq,
                    config.getTopicTag(), QueueOffsetCache.getMessageQueueOffset(mq),
                    config.getPullBatchSize());

                QueueOffsetCache.putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        BatchMessage msg = new BatchMessage(pullResult.getMsgFoundList(), mq);
                        batchCache.put(msg.getBatchId(), msg);
                        collector.emit(new Values(msg, msg.getBatchId()));
                        break;
                    case OFFSET_ILLEGAL:
                        throw new IllegalStateException("Illegal offset " + pullResult);
                    default:
                        LOG.warn("Unconcerned status {} for result {} !",
                            pullResult.getPullStatus(), pullResult);
                        break;
                }
            } catch (Exception e) {
                LOG.error("Error occurred in queue {} !", new Object[] {mq}, e);
            }
        }
    }

    @Override
    public void ack(final Object id) {
        BatchMessage batchMsgs = batchCache.remove(id);
        if (batchMsgs == null) {
            LOG.warn("Failed to get cached value from key {} !", id);
        }
    }

    @Override
    public void fail(final Object id) {
        BatchMessage msg = batchCache.get(id);

        int failureTimes = msg.getMessageStat().getFailureTimes().incrementAndGet();

        if (config.getMaxFailTimes() < 0 || failureTimes < config.getMaxFailTimes()) {
            collector.emit(new Values(msg, msg.getBatchId()));
        } else {
            LOG.warn("Skip message {}!", msg);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MessageExtList"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void setConfig(RocketMQConfig config) {
        this.config = config;
    }

}
