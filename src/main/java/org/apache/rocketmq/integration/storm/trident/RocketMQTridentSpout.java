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

package org.apache.rocketmq.integration.storm.trident;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.integration.storm.MessagePullConsumer;
import org.apache.rocketmq.integration.storm.domain.BatchMessage;
import org.apache.rocketmq.integration.storm.domain.RocketMQConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.spout.ISpoutPartition;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;


public class RocketMQTridentSpout
		implements IPartitionedTridentSpout<List<MessageQueue>, ISpoutPartition, BatchMessage> {

	private static final long serialVersionUID = 8972193358178718167L;

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQTridentSpout.class);

	private static final ConcurrentMap<String, List<MessageQueue>> CACHED_MESSAGE_QUEUE = new MapMaker().makeMap();
	private RocketMQConfig config;
	private volatile MessagePullConsumer consumer;

	public RocketMQTridentSpout() {
	}

	private MessagePullConsumer getMessagePullConsumer() {
		
		if (null == consumer) {
			
			config.setInstanceName(UUID.randomUUID().toString() + "_TridentPullConsumer");

			consumer = new MessagePullConsumer(config);
			
			try {
				consumer.start();
			} catch (Exception e) {
				LOG.error("Failed to start DefaultMQPullConsumer");
				throw new IllegalStateException(e);
			}
		}
		return consumer;
	}

	public RocketMQTridentSpout(RocketMQConfig config) throws MQClientException {
		this.config = config;
		try {
			Set<MessageQueue> mqs = getMessagePullConsumer().getConsumer().fetchSubscribeMessageQueues(config.getTopic());

			getMessagePullConsumer().getTopicQueueMappings().put(config.getTopic(), Lists.newArrayList(mqs));
		} catch (Exception e) {
			LOG.error("Error occurred !", e);
			throw new IllegalStateException(e);
		}
	}

	private List<MessageQueue> getMessageQueue(String topic) {
		List<MessageQueue> cachedQueue = getMessagePullConsumer().getTopicQueueMappings().get(config.getTopic());
		if (cachedQueue == null) {
			Set<MessageQueue> mqs;
			try {
				mqs = getMessagePullConsumer().getConsumer().fetchSubscribeMessageQueues(topic);
			} catch (MQClientException e) {
				LOG.error("Failed to fetch subscribed message queues", e);
				// TODO for now, just return an empty array list.
				return Lists.newArrayList();
			}

			cachedQueue = Lists.newArrayList(mqs);
			CACHED_MESSAGE_QUEUE.put(config.getTopic(), cachedQueue);
		}
		return cachedQueue;
	}

	class RocketMQCoordinator implements Coordinator<List<MessageQueue>> {

		@Override
		public List<MessageQueue> getPartitionsForBatch() {
			List<MessageQueue> queues = Lists.newArrayList();
			try {
				queues = getMessageQueue(config.getTopic());
			} catch (Exception e) {
				LOG.error("Failed to get subscribed message queue list", e);
			}
			return queues;
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public void close() {
			LOG.info("close coordinator!");
		}

	}

	class RocketMQEmitter implements Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> {

		@Override
		public List<ISpoutPartition> getOrderedPartitions(List<MessageQueue> allPartitionInfo) {
			List<ISpoutPartition> partition = Lists.newArrayList();
			for (final MessageQueue queue : allPartitionInfo) {
				partition.add(new ISpoutPartition() {
					@Override
					public String getId() {
						// BugFix: message queue ID is not unique if multiple
						// brokers are involved.
						// We need to guarantee its uniqueness among all message
						// queues, even spanning multiple brokers.
						return String.valueOf(getMessageQueue(config.getTopic()).indexOf(queue));
					}
				});
			}
			return partition;
		}

		private BatchMessage process(PullResult result, MessageQueue mq, TridentCollector collector,
				TransactionAttempt tx, BatchMessage lastPartitionMeta) throws MQClientException {
			switch (result.getPullStatus()) {
			case FOUND:
				BatchMessage batchMessages = null;
				List<MessageExt> msgs = result.getMsgFoundList();

				long max = Long.MIN_VALUE;
				long min = Long.MAX_VALUE;

				// TODO Assume DefaultMQPullConsumer has already filtered by
				// tag.

				if (null != msgs && msgs.size() > 0) {
					batchMessages = new BatchMessage(msgs, mq);
					for (MessageExt msg : msgs) {
						collector.emit(Lists.newArrayList(tx, msg));
						if (msg.getQueueOffset() > max) {
							max = msg.getQueueOffset();
						}

						if (msg.getQueueOffset() < min) {
							min = msg.getQueueOffset();
						}
					}

					batchMessages.setOffset(min);
					batchMessages.setNextOffset(result.getNextBeginOffset());
					// BugFix: pullResult.getMaxOffset is the largest consume
					// offset of the queue, instead of this pull batch.
					getMessagePullConsumer().getConsumer().updateConsumeOffset(mq, max);
				}
				return batchMessages;

			default:
				LOG.debug("None-FOUND Pull Status: {}", result.getPullStatus().name());
				return lastPartitionMeta;
			}
		}

		@Override
		public BatchMessage emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector,
				ISpoutPartition partition, BatchMessage lastPartitionMeta) {
			long index;
			BatchMessage batchMessages = null;
			MessageQueue mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));
			try {
				if (lastPartitionMeta == null) {
					index = getMessagePullConsumer().getConsumer().fetchConsumeOffset(mq, true);
					index = index == -1 ? 0 : index;
				} else {
					index = lastPartitionMeta.getNextOffset();
				}

				PullResult result = getMessagePullConsumer().getConsumer().pullBlockIfNotFound(mq, config.getTopicTag(),
						index, config.getPullBatchSize());
				batchMessages = process(result, mq, collector, tx, lastPartitionMeta);
			} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
				LOG.error("Pull message failed.", e);
			}
			return batchMessages;
		}

		@Override
		public void refreshPartitions(List<ISpoutPartition> partitionResponsibilities) {

		}

		@Override
		public void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, ISpoutPartition partition,
				BatchMessage partitionMeta) {
			MessageQueue mq;
			try {
				mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));
				PullResult result = getMessagePullConsumer().getConsumer().pullBlockIfNotFound(mq, config.getTopicTag(),
						partitionMeta.getOffset(), partitionMeta.getMsgList().size());
				process(result, mq, collector, tx, partitionMeta);
			} catch (Exception e) {
				LOG.error("Pull message failed.", e);
			}

		}

		@Override
		public void close() {
			LOG.info("close emitter!");
		}

	}

	@Override
	public Coordinator<List<MessageQueue>> getCoordinator(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context) {
		return new RocketMQCoordinator();
	}

	@Override
	public Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> getEmitter(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context) {
		return new RocketMQEmitter();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("tId", "message");
	}

}
