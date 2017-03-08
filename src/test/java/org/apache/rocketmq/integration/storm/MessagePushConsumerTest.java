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

import java.util.List;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Von Gosling
 */
public class MessagePushConsumerTest {

    private RocketMQConfig config;
    private MessageListener listener;
    private MessagePushConsumer consumer;

    @Before
    public void init() throws Exception {
        config = new RocketMQConfig();
        config.setInstanceName("rocketmq");
        config.setTopic("rocketmq-topic");
        config.setTopicTag("rocketmq-topic-tag");
        config.setGroupId("rocketmq-group");

        listener = new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        };
        consumer = new MessagePushConsumer(config);
        consumer.start(listener);
    }

    @After
    public void stop() {
        consumer.shutdown();
    }

    @Test
    public void testSuspend() throws Exception {
        consumer.suspend();
    }

    @Test
    public void testResume() throws Exception {
        consumer.resume();
    }

}
