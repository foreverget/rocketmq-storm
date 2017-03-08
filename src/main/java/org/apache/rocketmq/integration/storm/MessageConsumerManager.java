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

import org.apache.commons.lang3.BooleanUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.rocketmq.integration.storm.internal.tools.FastBeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Von Gosling
 */
public class MessageConsumerManager {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerManager.class);

    MessageConsumerManager() {
    }

    public static MQConsumer getConsumerInstance(RocketMQConfig config, MessageListener listener,
        Boolean isPushlet) throws MQClientException {
        LOG.info("Begin to init consumer,instanceName->{},configuration->{}",
            new Object[] {config.getInstanceName(), config});

        if (BooleanUtils.isTrue(isPushlet)) {
            DefaultMQPushConsumer pushConsumer = (DefaultMQPushConsumer) FastBeanUtils.copyProperties(config,
                DefaultMQPushConsumer.class);
            pushConsumer.setConsumerGroup(config.getGroupId());
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

            pushConsumer.subscribe(config.getTopic(), config.getTopicTag());
            if (listener instanceof MessageListenerConcurrently) {
                pushConsumer.registerMessageListener((MessageListenerConcurrently) listener);
            }
            if (listener instanceof MessageListenerOrderly) {
                pushConsumer.registerMessageListener((MessageListenerOrderly) listener);
            }
            return pushConsumer;
        } else {
            DefaultMQPullConsumer pullConsumer = (DefaultMQPullConsumer) FastBeanUtils.copyProperties(config,
                DefaultMQPullConsumer.class);
            pullConsumer.setConsumerGroup(config.getGroupId());

            return pullConsumer;
        }
    }
}
