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
package com.pinterest.secor.common;

import com.pinterest.secor.message.ParsedMessage;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * LogFilePath represents path of a log file.  It contains convenience method for building and
 * decomposing paths.
 *
 * Log file path has the following form:
 *     prefix/topic/partition1/.../partitionN/generation_kafkaParition_firstMessageOffset
 * where:
 *     prefix is top-level directory for log files.  It can be a local path or an s3 dir,
 *     topic is a kafka topic,
 *     partition1, ..., partitionN is the list of partition names extracted from message content.
 *         E.g., the partition may describe the message date such as dt=2014-01-01,
 *     generation is the consumer version.  It allows up to perform rolling upgrades of
 *         non-compatible Secor releases,
 *     kafkaPartition is the kafka partition of the topic,
 *     firstMessageOffset is the offset of the first message in a batch of files committed
 *         atomically.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFilePath {
    private static final String DEFAULT_DELIMITER = "_";
    private String mPrefix;
    private String mTopic;
    private Partitions mPartitions;
    private int mGeneration;
    private int mKafkaPartition;
    private long mOffset;
    private String mExtension;
    private String mDelimiter;

    public static LogFilePath createFromPath(String prefix, String path, String delimiter) {
        assert path.startsWith(prefix): path + ".startsWith(" + prefix + ")";

        int prefixLength = prefix.length();
        if (!prefix.endsWith("/")) {
            prefixLength++;
        }
        String suffix = path.substring(prefixLength);
        String[] pathElements = suffix.split("/");
        // Suffix should contain a topic, at least one partition, and the basename.
        assert pathElements.length >= 3: Arrays.toString(pathElements) + ".length >= 3";

        String topic = pathElements[0];
        List<String> pathPartitions = Arrays.asList(Arrays.copyOfRange(pathElements, 0, pathElements.length - 1));
        String extension;

        // Parse basename.
        String basename = pathElements[pathElements.length - 1];
        // Remove extension.
        int lastIndexOf = basename.lastIndexOf('.');
        if (lastIndexOf >= 0) {
            extension = basename.substring(lastIndexOf, basename.length());
            basename = basename.substring(0, lastIndexOf);
        } else {
            extension = "";
        }
        List<String> basenameElements = new ArrayList<String>(Arrays.asList(basename.split(delimiter)));
        assert basenameElements.size() == 3: Integer.toString(basenameElements.size()) + " == 3";
        int generation = Integer.parseInt(basenameElements.get(0));
        int kafkaPartition = Integer.parseInt(basenameElements.get(1));
        long offset = Long.parseLong(basenameElements.get(2));
        Partitions partitions = new Partitions(pathPartitions, basenameElements.subList(0, 1));
        return new LogFilePath(prefix, topic, partitions, generation, kafkaPartition, offset, extension, delimiter);
    }

    public static LogFilePath createFromPath(String prefix, String path) {
      return createFromPath(prefix, path, DEFAULT_DELIMITER);
    }

    public static String defaultOffsetFormat(long offset) {
        return String.format("%020d", offset);
    }

    public LogFilePath(String prefix, String topic, Partitions partitions, int generation,
                       int kafkaPartition, long offset, String extension, String delimiter) {
        mPrefix = prefix;
        mTopic = topic;
        mPartitions = partitions;
        mGeneration = generation;
        mKafkaPartition = kafkaPartition;
        mOffset = offset;
        mExtension = extension;
        mDelimiter = delimiter;
    }

    public LogFilePath(String prefix, String topic, Partitions partitions, int generation,
                       int kafkaPartition, long offset, String extension) {
        this(prefix, topic, partitions, generation, kafkaPartition, offset, extension, DEFAULT_DELIMITER);
    }

    public LogFilePath(String prefix, int generation, long lastCommittedOffset,
                       ParsedMessage message, String extension) {
        this(prefix, message.getTopic(), message.getPartitions(), generation,
             message.getKafkaPartition(), lastCommittedOffset, extension, DEFAULT_DELIMITER);
    }

    public LogFilePath(String prefix, int generation, long lastCommittedOffset,
                       ParsedMessage message, String extension, String delimiter) {
        this(prefix, message.getTopic(), message.getPartitions(), generation,
             message.getKafkaPartition(), lastCommittedOffset, extension, delimiter);
    }

    public String getLogFileParentDir() {
        List<String> elements = new ArrayList<String>();
        elements.add(mPrefix);
        return StringUtils.join(elements, "/");
    }

    public String getLogFileDir() {
        List<String> elements = new ArrayList<String>();
        elements.add(getLogFileParentDir());
        elements.addAll(mPartitions.getPathPartitions());
        return StringUtils.join(elements, "/");
    }

    public String getLogFilePath() {
        List<String> pathElements = new ArrayList<String>();
        pathElements.add(getLogFileDir());
        pathElements.add(getLogFileBasename());
        return StringUtils.join(pathElements, "/") + mExtension;
    }

    public String getLogFileCrcPath() {
        String basename = "." + getLogFileBasename() + ".crc";
        List<String> pathElements = new ArrayList<String>();
        pathElements.add(getLogFileDir());
        pathElements.add(basename);
        return StringUtils.join(pathElements, "/");
    }

    public String getTopic() {
        return mTopic;
    }

    public Partitions getPartitions() {
        return mPartitions;
    }

    public int getGeneration() {
        return mGeneration;
    }

    public int getKafkaPartition() {
        return mKafkaPartition;
    }

    public long getOffset() {
        return mOffset;
    }

    public String getExtension() {
        return mExtension;
    }

    public String getDelimiter() {
        return mDelimiter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogFilePath that = (LogFilePath) o;

        if (mGeneration != that.mGeneration) return false;
        if (mKafkaPartition != that.mKafkaPartition) return false;
        if (mOffset != that.mOffset) return false;
        if (!mPartitions.equals(that.mPartitions)) return false;
        if (mPrefix != null ? !mPrefix.equals(that.mPrefix) : that.mPrefix != null) return false;
        if (mTopic != null ? !mTopic.equals(that.mTopic) : that.mTopic != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = mPrefix != null ? mPrefix.hashCode() : 0;
        result = 31 * result + (mTopic != null ? mTopic.hashCode() : 0);
        result = 31 * result + (mPartitions != null ? mPartitions.hashCode() : 0);
        result = 31 * result + mGeneration;
        result = 31 * result + mKafkaPartition;
        result = 31 * result + (int) (mOffset ^ (mOffset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return getLogFilePath();
    }

    private String getLogFileBasename() {
        List<String> basenameElements = new ArrayList<String>();
        basenameElements.addAll(mPartitions.getFilenamePartitions());
        basenameElements.add(Integer.toString(mKafkaPartition));
        basenameElements.add(defaultOffsetFormat(mOffset));
        return StringUtils.join(basenameElements, mDelimiter);
    }
}
