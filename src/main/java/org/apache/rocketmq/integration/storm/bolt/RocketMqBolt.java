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

package org.apache.rocketmq.integration.storm.bolt;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqBolt implements IRichBolt {
	
    private static final long serialVersionUID = 7591260982890048043L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMqBolt.class);

    private OutputCollector collector;
    
    /**
     * 在初始化的时候，只执行一次
     */
    @Override
    @SuppressWarnings("rawtypes") 
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    /**
     * 逻辑执行方法
     */
    @Override
    public void execute(Tuple input) {
        Object msgObj = input.getValue(0);
        Object msgStat = input.getValue(1);
        try {
        	// 处理消息
            LOG.info("Messages:" + msgObj + "\n statistics:" + msgStat);

        } catch (Exception e) {
            collector.fail(input);
            return;
            //throw new FailedException(e);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
