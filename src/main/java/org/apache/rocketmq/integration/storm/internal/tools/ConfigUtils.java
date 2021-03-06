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

package org.apache.rocketmq.integration.storm.internal.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.lang.BooleanUtils;

/**
 * Utilities for RocketMQ spout regarding its configuration and reading values
 * from the storm configuration.
 */
public abstract class ConfigUtils {
	
    /**
     * Storm configuration key pointing to a file containing rocketmq
     * configuration ({@code "rocketmq.config"}).
     */
    public static final String CONFIG_FILE = "rocketmq.config";
    
    public static final String CONFIG_NAMESRV_ADDR = "rocketmq.spout.namesrv.addr";
    /**
     * Storm configuration key used to determine the rocketmq topic to read from
     * ( {@code "rocketmq.spout.topic"}).
     */
    public static final String CONFIG_TOPIC = "rocketmq.spout.topic";
    /**
     * Default rocketmq topic to read from ({@code "rocketmq_spout_topic"}).
     */
    public static final String CONFIG_DEFAULT_TOPIC = "rocketmq_spout_topic";
    /**
     * Storm configuration key used to determine the rocketmq consumer group (
     * {@code "rocketmq.spout.consumer.group"}).
     */
    public static final String CONFIG_CONSUMER_GROUP = "rocketmq.spout.consumer.group";
    /**
     * Default rocketmq consumer group id (
     * {@code "rocketmq_spout_consumer_group"}).
     */
    public static final String CONFIG_DEFAULT_CONSUMER_GROUP = "rocketmq_spout_consumer_group";
    /**
     * Storm configuration key used to determine the rocketmq topic tag(
     * {@code "rocketmq.spout.topic.tag"}).
     */
    public static final String CONFIG_TOPIC_TAG = "rocketmq.spout.topic.tag";

    public static final String CONFIG_ROCKETMQ = "rocketmq.config";

    public static final String CONFIG_PREFETCH_SIZE = "rocketmq.prefetch.size";

    /**
     * Reads configuration from a classpath resource stream obtained from the
     * current thread's class loader through
     * {@link ClassLoader#getSystemResourceAsStream(String)}.
     *
     * @param resource The resource to be read.
     * @return A {@link java.util.Properties} object read from the specified resource.
     * @throws IllegalArgumentException When the configuration file could not be found or another I/O error occurs.
     */
    public static Properties getResource(final String resource) {
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (input == null) {
            throw new IllegalArgumentException("configuration file '" + resource + "' not found on classpath");
        }

        final Properties config = new Properties();
        try {
            config.load(input);
        } catch (final IOException e) {
            throw new IllegalArgumentException("reading configuration from '" + resource + "' failed", e);
        }
        return config;
    }

    public static Config init(String configFile) {
        Config config = new Config();
        //1.获取配置文件
        Properties prop = ConfigUtils.getResource(configFile);
        // 复制配置文件属性到config对象中
        for (Entry<Object, Object> entry : prop.entrySet()) {
            config.put((String) entry.getKey(), entry.getValue());
        }
        //2.获取主题、消费者组、主题Tag
        String topic = (String) config.get(ConfigUtils.CONFIG_TOPIC);
        String consumerGroup = (String) config.get(ConfigUtils.CONFIG_CONSUMER_GROUP);
        String topicTag = (String) config.get(ConfigUtils.CONFIG_TOPIC_TAG);
        String namesrvAddr = (String) config.get(ConfigUtils.CONFIG_NAMESRV_ADDR);
        
        
        //Integer pullBatchSize = (Integer) config.get(ConfigUtils.CONFIG_PREFETCH_SIZE);
        // 拉取消息数量
        Integer pullBatchSize = Integer.parseInt((String) config.get(ConfigUtils.CONFIG_PREFETCH_SIZE));

        // 组装MQ配置对象
        RocketMQConfig mqConfig = new RocketMQConfig(namesrvAddr,consumerGroup, topic, topicTag);

        if (pullBatchSize > 0) {
            mqConfig.setPullBatchSize(pullBatchSize);
        }
        
		boolean ordered = BooleanUtils
				.toBooleanDefaultIfNull(Boolean.valueOf((String) (config.get("rocketmq.spout.ordered"))), false);
        mqConfig.setOrdered(ordered);
        // key:rocketmq.config
        config.put(CONFIG_ROCKETMQ, mqConfig);
        return config;
    }

    private ConfigUtils() {
    }
}
