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

package org.apache.rocketmq.integration.storm.domain;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


public class BatchMessage {
	
    private final static long WAIT_TIMEOUT = 5;

    private UUID batchId;
    /**
     * No need not store these messages into zookeeper in case of Trident topology.
     */
    private transient List<MessageExt> msgList;
    private MessageQueue mq;

    private CountDownLatch latch;
    private boolean isSuccess;

    private long nextOffset;
    private long offset;

    private MessageStat messageStat = new MessageStat();

    public BatchMessage() {
    }

    public BatchMessage(List<MessageExt> msgList, MessageQueue mq) {
        this.msgList = msgList;
        this.mq = mq;

        this.batchId = UUID.randomUUID();
        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
    }

    public UUID getBatchId() {
        return batchId;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }

    public boolean waitFinish() throws InterruptedException {
        return latch.await(WAIT_TIMEOUT, TimeUnit.MINUTES);
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public String getMessageQueue() {
        if (mq == null) {
            return null;
        }
        return mq.toString();
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public MessageStat getMessageStat() {
        return messageStat;
    }

    public void setMessageStat(MessageStat messageStat) {
        this.messageStat = messageStat;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
