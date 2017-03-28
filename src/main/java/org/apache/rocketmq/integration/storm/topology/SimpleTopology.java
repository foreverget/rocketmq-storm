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

package org.apache.rocketmq.integration.storm.topology;

import org.apache.rocketmq.integration.storm.bolt.RocketMqBolt;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.rocketmq.integration.storm.domain.RocketMQSpouts;
import org.apache.rocketmq.integration.storm.internal.tools.ConfigUtils;
import org.apache.rocketmq.integration.storm.spout.StreamMessageSpout;
import org.apache.rocketmq.integration.storm.spout.factory.RocketMQSpoutFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.shade.org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopology {
	
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopology.class);

    private static final String BOLT_NAME = "MQBolt";
    
    private static final String PROP_FILE_NAME = "mqspout.default.prop";

    private static Config config = new Config();
    
    private static boolean isLocalMode = true;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = buildTopology(ConfigUtils.init(PROP_FILE_NAME));
        submitTopology(builder);
    }

    private static TopologyBuilder buildTopology(Config config) throws Exception {
    	
        TopologyBuilder builder = new TopologyBuilder();

        int boltParallel = NumberUtils.toInt((String) config.get("topology.bolt.parallel"), 1);

        int spoutParallel = NumberUtils.toInt((String) config.get("topology.spout.parallel"), 1);

        BoltDeclarer writerBolt = builder.setBolt(BOLT_NAME, new RocketMqBolt(), boltParallel);

        StreamMessageSpout defaultSpout = (StreamMessageSpout) RocketMQSpoutFactory.getSpout(RocketMQSpouts.STREAM.getValue());
        RocketMQConfig mqConig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);
        defaultSpout.setConfig(mqConig);

        String id = (String) config.get(ConfigUtils.CONFIG_TOPIC);
        builder.setSpout(id, defaultSpout, spoutParallel);

        writerBolt.shuffleGrouping(id);
        return builder;
    }

    private static void submitTopology(TopologyBuilder builder) {
        try {
            if (isLocalMode) {
                LocalCluster cluster = new LocalCluster();

                config.put(Config.STORM_CLUSTER_MODE, "local");
                cluster.submitTopology(String.valueOf(config.get("topology.name")), config,
                    builder.createTopology());

                Thread.sleep(50000);

                cluster.shutdown();
            } else {
                config.put(Config.STORM_CLUSTER_MODE, "distributed");
                StormSubmitter.submitTopology(String.valueOf(config.get("topology.name")), config,
                    builder.createTopology());
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e.getCause());
        }
    }
}
