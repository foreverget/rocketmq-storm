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

import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.rocketmq.integration.storm.internal.tools.FastBeanUtils;
import org.apache.storm.shade.org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * 消息消费者管理
 *
 */
public class MessageConsumerManager {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerManager.class);

    MessageConsumerManager() {
    }

    /**
     * 
     * @param config MQ配置
     * @param listener 消息监听
     * @param isPushlet 是否是推模型
     * @return
     * @throws MQClientException
     */
    public static MQConsumer getConsumerInstance(RocketMQConfig config, MessageListener listener,
        Boolean isPushlet) throws MQClientException {
        LOG.info("Begin to init consumer,instanceName->{},configuration->{}",
            new Object[] {config.getInstanceName(), config});

        if (BooleanUtils.isTrue(isPushlet)) {
            DefaultMQPushConsumer pushConsumer = (DefaultMQPushConsumer) FastBeanUtils.copyProperties(config,
                DefaultMQPushConsumer.class);
            pushConsumer.setNamesrvAddr(config.getNamesrvAddr());
            pushConsumer.setConsumerGroup(config.getGroupId());
            // Consumer从哪里开始消费
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            // 消费者订阅主题及Tag
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
            pullConsumer.setNamesrvAddr(config.getNamesrvAddr());
            return pullConsumer;
        }
    }
}
