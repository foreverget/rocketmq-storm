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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ServiceLoader;
import org.apache.rocketmq.integration.storm.domain.RocketMQSpouts;
import org.apache.rocketmq.integration.storm.annotation.Extension;
import org.apache.storm.topology.IRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RocketMQSpoutFactory {
	
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSpoutFactory.class);

	private static IRichSpout spout;

	private static Cache<String, IRichSpout> cache = CacheBuilder.newBuilder().build();

	private static final String DEFAULT_BROKER = RocketMQSpouts.STREAM.getValue();

	public static IRichSpout getSpout(String spoutName) {
		RocketMQSpouts spoutType = RocketMQSpouts.fromString(spoutName);
		switch (spoutType) {
		case SIMPLE:
		case BATCH:
		case STREAM:
			return locateSpout(spoutName);
		default:
			LOG.warn("Can not support this spout type {} temporarily !", spoutName);
			return locateSpout(DEFAULT_BROKER);

		}
	}

	private static IRichSpout locateSpout(String spoutName) {
		spout = cache.getIfPresent(spoutName);
		if (null == spout) {
			for (IRichSpout spoutInstance : ServiceLoader.load(IRichSpout.class)) {
				Extension ext = spoutInstance.getClass().getAnnotation(Extension.class);
				if (spoutName.equals(ext.value())) {
					spout = spoutInstance;
					cache.put(spoutName, spoutInstance);
					return spoutInstance;
				}
			}
		}

		return spout;
	}

}
