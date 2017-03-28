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

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;

import mockit.Mocked;


public class MessageConsumerManagerTest {

    private RocketMQConfig config;

    @Mocked
    private MessageListener listener;

    @Before
    public void init() {
        config = new RocketMQConfig();
        config.setInstanceName("rocketmq");
        config.setTopic("rocketmq-topic");
        config.setTopicTag("rocketmq-topic-tag");
        config.setGroupId("rocketmq-group");
    }

    @Test
    public void testGetConsumerInstance() throws Exception {
        MQConsumer consumer = MessageConsumerManager.getConsumerInstance(config, listener, true);
        assertThat(consumer, instanceOf(DefaultMQPushConsumer.class));

        consumer = MessageConsumerManager.getConsumerInstance(config, listener, false);
        assertThat(consumer, instanceOf(DefaultMQPullConsumer.class));
    }
}
