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

import java.io.Serializable;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;

public class MessagePushConsumer implements Serializable {
	
    private static final long serialVersionUID = 4641537253577312163L;

    private static final Logger LOG = LoggerFactory.getLogger(MessagePushConsumer.class);

    private final RocketMQConfig config;

    private transient DefaultMQPushConsumer consumer;

    public MessagePushConsumer(RocketMQConfig config) {
        this.config = config;
    }

    public void start(MessageListener listener) throws Exception {
        consumer = (DefaultMQPushConsumer) MessageConsumerManager.getConsumerInstance(config,listener, true);
        this.consumer.start();
        LOG.info("Init consumer successfully,configuration->{} !", config);
    }

    public void shutdown() {
        consumer.shutdown();
        LOG.info("Successfully shutdown consumer {} !", config);
    }

    public void suspend() {
        consumer.suspend();
        LOG.info("Pause consumer !");
    }

    public void resume() {
        consumer.resume();
        LOG.info("Resume consumer !");
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }
}
