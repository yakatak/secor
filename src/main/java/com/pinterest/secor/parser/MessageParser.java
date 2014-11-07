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
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.Partitions;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

// TODO(pawel): should we offer a multi-message parser capable of parsing multiple types of
// messages?  E.g., it could be implemented as a composite trying out different parsers and using
// the one that works.  What is the performance cost of such approach?

/**
 * Message parser extracts partitions from messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public abstract class MessageParser {
    protected SecorConfig mConfig;

    public MessageParser(SecorConfig config) {
        mConfig = config;
    }

    public ParsedMessage parse(Message message) throws Exception {
        return new ParsedMessage(message.getTopic(), message.getKafkaPartition(),
                                 message.getOffset(), message.getPayload(), extract(message));
    }

    public abstract String[] extractPartitions(Message payload) throws Exception;

    protected Partitions extract(Message message) throws Exception {
        List<String> pathPartitions = new ArrayList<String>(Arrays.asList(extractPartitions(message)));
        pathPartitions.add(0, message.getTopic());
        List<String> filenamePartitions = Arrays.asList(extractFilenamePartitions(message));
        return new Partitions(pathPartitions, filenamePartitions);
    }

    protected String[] extractFilenamePartitions(Message message) {
        String[] partitions = new String[3];
        partitions[0] = Integer.toString(mConfig.getGeneration());
        partitions[1] = Integer.toString(message.getKafkaPartition());
        partitions[2] = String.format("%020d", message.getOffset());
        return partitions;
    }
}
