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

package org.apache.rocketmq.integration.storm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Von Gosling
 */
public class MessagePullConsumer implements Serializable, MessageQueueListener {
    private static final long serialVersionUID = 4641537253577312163L;

    private static final Logger LOG = LoggerFactory.getLogger(MessagePullConsumer.class);

    private final RocketMQConfig config;

    private transient DefaultMQPullConsumer consumer;

    private Map<String, List<MessageQueue>> topicQueueMappings = Maps.newHashMap();

    public Map<String, List<MessageQueue>> getTopicQueueMappings() {
        return topicQueueMappings;
    }

    public MessagePullConsumer(RocketMQConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(config, null, false);

        consumer.registerMessageQueueListener(config.getTopic(), this);

        this.consumer.start();

        LOG.info("Init consumer successfully,configuration->{} !", config);
    }

    public void shutdown() {
        consumer.shutdown();

        LOG.info("Successfully shutdown consumer {} !", config);
    }

    public void suspend() {
        LOG.info("Nothing to do for pull consumer !");
    }

    public void resume() {
        LOG.info("Nothing to do for pull consumer !");
    }

    public DefaultMQPullConsumer getConsumer() {
        return consumer;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll,
        Set<MessageQueue> mqDivided) {
        if (topicQueueMappings.get(topic) != null) {

            LOG.info("Queue changed from {} to {} !", mqAll, mqDivided);

            topicQueueMappings.put(topic, Lists.newArrayList(mqDivided));
        }
    }
}
