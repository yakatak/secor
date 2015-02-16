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
import com.pinterest.secor.message.Message;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.UUID;

/**
 * Thrift message parser extracts date partitions from thrift messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ThriftMessageParser extends TimestampedMessageParser {

    private TimestampExtractor mTimestampExtractor;

    class TimestampExtractor {
      private final TDeserializer mDeserializer;
      private final TFieldIdEnum mFieldIdEnum;
      private final ThriftTimestampReader mThriftTimestampReader;
      public TimestampExtractor(TDeserializer deserializer, TFieldIdEnum fieldIdEnum, ThriftTimestampReader thriftTimestampReader) {
        this.mDeserializer = deserializer;
        this.mFieldIdEnum = fieldIdEnum;
        this.mThriftTimestampReader = thriftTimestampReader;
      }

      public long getTimestamp(Message message) throws TException {
        return mThriftTimestampReader.readThriftTimestamp(
            mDeserializer,
            mFieldIdEnum,
            message.getPayload());
      }
    }

    class LongThriftTimestampReader implements ThriftTimestampReader {
      public long readThriftTimestamp(TDeserializer deserializer, TFieldIdEnum fieldIdEnum, byte[] payload) throws TException {
          return deserializer.partialDeserializeI64(payload, fieldIdEnum);
      }
    }

    class UUIDThriftTimestampReader implements ThriftTimestampReader {
      public long readThriftTimestamp(TDeserializer deserializer, TFieldIdEnum fieldIdEnum, byte[] payload) throws TException {
          final UUID uuid = UUID.fromString(
              deserializer.partialDeserializeString(payload, fieldIdEnum));
          return uuid.timestamp();
      }
    }

    public ThriftMessageParser(SecorConfig config) {
        super(config);
        try {
          final TProtocolFactory protocolFactory =
            ((Class<TProtocolFactory>) Class.forName("org.apache.thrift.protocol." + mConfig.getThriftProtocolName() + ".Factory"))
              .newInstance();
          final TDeserializer deserializer = new TDeserializer(protocolFactory);
          final TFieldIdEnum messageTimestampField = mConfig.getMessageTimestampThriftField();
          final String thriftFieldType = config.getMessageTimestampThriftFieldType();
          switch (thriftFieldType) {
            case "timeuuid":
              this.mTimestampExtractor = new TimestampExtractor(deserializer, messageTimestampField, new UUIDThriftTimestampReader());
              break;
            case "i64":
              this.mTimestampExtractor = new TimestampExtractor(deserializer, messageTimestampField, new LongThriftTimestampReader());
              break;
            default: 
              throw new RuntimeException("Unknown field ID type:" + thriftFieldType);
          }
        } catch (ClassNotFoundException cnfe) {
          throw new RuntimeException("ClassNotFoundException", cnfe);
        } catch (InstantiationException ie) {
          throw new RuntimeException("InstantiationException", ie);
        } catch (IllegalAccessException iae) {
          throw new RuntimeException("IllegalAccessException", iae);
        }
    }

    @Override
    public long extractTimestampMillis(final Message message) throws TException {
      return mTimestampExtractor.getTimestamp(message);
    }
}
