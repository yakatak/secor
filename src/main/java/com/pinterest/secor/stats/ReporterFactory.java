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
package com.pinterest.secor.stats;

import org.apache.commons.configuration.Configuration;
import java.lang.ClassNotFoundException;
import java.lang.InstantiationException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.LinkedList;
import java.util.Properties;

/**
 * StatsFactory is responsible for creating StatsReporter objects, given a proper
 * configuration.
 *
 * @author Mathias Söderberg (mathias@burtcorp.com)
 */
public class ReporterFactory {
    public static List<StatsReporter> createReporters(Configuration config) throws Exception {
        List<StatsReporter> reporters = new LinkedList<StatsReporter>();
        if (config.containsKey("reporters")) {
            Properties reportersProps = config.getProperties("reporters");
            for(Object objectReporterId : reportersProps.keySet()) {
                String reporterId = (String) objectReporterId;
                StatsReporter reporter = createReporter(reportersProps.getProperty(reporterId), config.subset(reporterId));
                reporters.add(reporter);
            }
        }
        return reporters;
    }

    private static StatsReporter createReporter(String className, Configuration config) throws Exception {
        Class<?> clazz = Class.forName(className);
        for (Constructor<?> ctor : clazz.getConstructors()) {
            Class<?>[] paramTypes = ctor.getParameterTypes();

            if (paramTypes.length == 1) {
                Object[] args = { config };
                return ((StatsReporter) ctor.newInstance(args));
            }
        }
        throw new IllegalArgumentException("Class '" + className + "' could not be found");
    }
}
