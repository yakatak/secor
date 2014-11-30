/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.tools;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.pinterest.secor.common.KafkaClient;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.parser.TimestampedMessageParser;
import com.pinterest.secor.stats.StatsReporter;
import com.pinterest.secor.stats.ReporterFactory;
import com.pinterest.secor.stats.Stat;
import com.pinterest.secor.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

/**
 * Progress monitor exports offset lags per topic partition.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ProgressMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressMonitor.class);
    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private KafkaClient mKafkaClient;
    private MessageParser mMessageParser;
    private List<StatsReporter> mStatsReporters;

    public ProgressMonitor(SecorConfig config)
            throws Exception
    {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mKafkaClient = new KafkaClient(mConfig);
        mMessageParser = (MessageParser) ReflectionUtil.createMessageParser(
                mConfig.getMessageParserClass(), mConfig);
        mStatsReporters = ReporterFactory.createReporters(mConfig.getStatsConfig());
    }

    public void exportStats() throws Exception {
        List<Stat> stats = getStats();
        for(StatsReporter reporter : mStatsReporters) {
            reporter.report(stats);
        }
    }

    private List<Stat> getStats() throws Exception {
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        List<Stat> stats = Lists.newArrayList();

        for (String topic : topics) {
            if (topic.matches(mConfig.getMonitoringBlacklistTopics()) ||
                    !topic.matches(mConfig.getKafkaTopicFilter())) {
                LOG.info("skipping topic " + topic);
                continue;
            }
            List<Integer> partitions = mZookeeperConnector.getCommittedOffsetPartitions(topic);
            for (Integer partition : partitions) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                Message committedMessage = mKafkaClient.getCommittedMessage(topicPartition);
                long committedOffset = - 1;
                long committedTimestampMillis = -1;
                if (committedMessage == null) {
                    LOG.warn("no committed message found in topic " + topic + " partition " +
                        partition);
                } else {
                    committedOffset = committedMessage.getOffset();
                    committedTimestampMillis = getTimestamp(committedMessage);
                }

                Message lastMessage = mKafkaClient.getLastMessage(topicPartition);
                if (lastMessage == null) {
                    LOG.warn("no message found in topic " + topic + " partition " + partition);
                } else {
                    long lastOffset = lastMessage.getOffset();
                    long lastTimestampMillis = getTimestamp(lastMessage);
                    assert committedOffset <= lastOffset: Long.toString(committedOffset) + " <= " +
                        lastOffset;

                    long offsetLag = lastOffset - committedOffset;
                    long timestampMillisLag = lastTimestampMillis - committedTimestampMillis;
                    Map<String, String> tags = ImmutableMap.of(
                            "topic", topic,
                            "partition", Integer.toString(partition)
                    );

                    long timestamp = System.currentTimeMillis() / 1000;
                    stats.add(Stat.createInstance("secor.lag.offsets", tags, Long.toString(offsetLag), timestamp));
                    stats.add(Stat.createInstance("secor.lag.seconds", tags, Long.toString(timestampMillisLag / 1000), timestamp));

                    LOG.debug("topic " + topic + " partition " + partition + " committed offset " +
                        committedOffset + " last offset " + lastOffset + " committed timestamp " +
                            (committedTimestampMillis / 1000) + " last timestamp " +
                            (lastTimestampMillis / 1000));
                }
            }
        }

        return stats;
    }

    private long getTimestamp(Message message) throws Exception {
        if (mMessageParser instanceof TimestampedMessageParser) {
            return ((TimestampedMessageParser)mMessageParser).extractTimestampMillis(message);
        } else {
            return -1;
        }
    }
}
