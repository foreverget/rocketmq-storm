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

import org.apache.rocketmq.integration.storm.bolt.RocketMqBolt;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.rocketmq.integration.storm.domain.RocketMQSpouts;
import org.apache.rocketmq.integration.storm.internal.tools.ConfigUtils;
import org.apache.rocketmq.integration.storm.spout.SimpleMessageSpout;
import org.apache.rocketmq.integration.storm.spout.factory.RocketMQSpoutFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopologyDemo {
	
	private static final Logger LOG = LoggerFactory.getLogger(SimpleTopologyDemo.class);

	private static final String BOLT_NAME = "notifier";
	private static final String PROP_FILE_NAME = "mqspout.test.prop";

	private static Config conf = new Config();
	private static boolean isLocalMode = true;

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = buildTopology(ConfigUtils.init(PROP_FILE_NAME));

		submitTopology(builder);
	}

	/**
	 * 创建拓扑
	 * 
	 * @param config
	 * @return
	 * @throws Exception
	 */
	private static TopologyBuilder buildTopology(Config config) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();

		// 并发数默认为1（Executor 线程数）
		int boltParallel = NumberUtils.toInt((String) config.get("topology.bolt.parallel"), 1);

		int spoutParallel = NumberUtils.toInt((String) config.get("topology.spout.parallel"), 1);
		
		// 定义Bolt
		BoltDeclarer writerBolt = builder.setBolt(BOLT_NAME, new RocketMqBolt(), boltParallel);
		
		// 创建默认spout
		SimpleMessageSpout defaultSpout = (SimpleMessageSpout) RocketMQSpoutFactory.getSpout(RocketMQSpouts.SIMPLE.getValue());
		// 获取MQ配置对象
		RocketMQConfig mqConig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);
		
		defaultSpout.setConfig(mqConig);

		String id = (String) config.get(ConfigUtils.CONFIG_TOPIC);
		
		// 定义spout，id为指定的topic值
		builder.setSpout(id, defaultSpout, spoutParallel);

		// 随机分发到writerBolt
		writerBolt.shuffleGrouping(id);
		
		return builder;
	}

	/**
	 * 提交拓扑
	 * 
	 * @param builder
	 */
	private static void submitTopology(TopologyBuilder builder) {
		try {
			// 获取拓扑名称
			String topologyName = String.valueOf(conf.get("topology.name"));
			// 创建拓扑
			StormTopology topology = builder.createTopology();
			
			// 本地模式
			if (isLocalMode == true) {
				LocalCluster cluster = new LocalCluster();
				
				conf.put(Config.STORM_CLUSTER_MODE, "local");
				
				cluster.submitTopology(topologyName, conf, topology);

				Thread.sleep(50000);
				// 停止拓扑
				cluster.killTopology(topologyName);
				// 停止集群
				cluster.shutdown();
			} else {
				conf.put(Config.STORM_CLUSTER_MODE, "distributed");
				StormSubmitter.submitTopology(topologyName, conf, topology);
			}

		} catch (Exception e) {
			LOG.error(e.getMessage(), e.getCause());
		}
	}
}
