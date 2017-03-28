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

package org.apache.rocketmq.integration.storm.spout.factory;

import org.apache.rocketmq.integration.storm.domain.RocketMQSpouts;
import org.apache.rocketmq.integration.storm.spout.BatchMessageSpout;
import org.apache.rocketmq.integration.storm.spout.SimpleMessageSpout;
import org.apache.rocketmq.integration.storm.spout.StreamMessageSpout;
import org.apache.storm.topology.IRichSpout;
import org.junit.Test;

import static org.hamcrest.core.Is.isA;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

public class RocketMQSpoutFactoryTest {

    @Test(expected = java.lang.NullPointerException.class)
    public void getNullSpoutTest() throws Exception {
        RocketMQSpoutFactory.getSpout(null);
    }

    @Test
    public void getSpoutTest() throws Exception {
        IRichSpout richSpout = RocketMQSpoutFactory.getSpout(RocketMQSpouts.SIMPLE.getValue());
        assertThat(richSpout, isA(IRichSpout.class));
        assertThat(richSpout, instanceOf(SimpleMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(RocketMQSpouts.BATCH.getValue());
        assertThat(richSpout, instanceOf(BatchMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(RocketMQSpouts.STREAM.getValue());
        assertThat(richSpout, instanceOf(StreamMessageSpout.class));
    }
}
