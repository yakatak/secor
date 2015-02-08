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

import com.pinterest.secor.message.Message;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Partitions represents ...
 *
 * @author Mathias Söderberg (mathias@burtcorp.com)
 */
public class Partitions {
    private List<String> mPathPartitions;
    private List<String> mFilenamePartitions;

    public static List<String> defaultPathPartitions(String topic, String[] partitions) {
        List<String> pathPartitions = new ArrayList<String>(partitions.length + 1);
        pathPartitions.add(topic);
        pathPartitions.addAll(Arrays.asList(partitions));
        return pathPartitions;
    }

    public static String[] defaultFilenamePartitions(int generation, int partition) {
        String[] partitions = {
            Integer.toString(generation)
        };
        return partitions;
    }

    public Partitions(List<String> pathPartitions, List<String> filenamePartitions) {
        mPathPartitions = pathPartitions;
        mFilenamePartitions = filenamePartitions;
    }

    public List<String> getPathPartitions() {
        return mPathPartitions;
    }

    public List<String> getFilenamePartitions() {
        return mFilenamePartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partitions that = (Partitions) o;

        return mPathPartitions.equals(that.getPathPartitions()) &&
          mFilenamePartitions.equals(that.getFilenamePartitions());
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (mPathPartitions != null ? mPathPartitions.hashCode() : 0);
        result = 31 * result + (mFilenamePartitions != null ? mFilenamePartitions.hashCode() : 0);
        return result;
    }
}
