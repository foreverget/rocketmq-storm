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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringStyle;



public class MessageStat implements Serializable {
	
    private static final long serialVersionUID = 1277714452693486955L;

    private AtomicInteger failureTimes = new AtomicInteger(0);
    
    private long elapsedTime = System.currentTimeMillis();

    public MessageStat() {
        super();
    }

    public MessageStat(int failureTimes) {
        this.failureTimes = new AtomicInteger(failureTimes);
    }

    public void setElapsedTime() {
        this.elapsedTime = System.currentTimeMillis();
    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
