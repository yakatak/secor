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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;
import org.apache.commons.configuration.Configuration;

public class OpenTSDBReporter implements StatsReporter {
    private String mHostAndPort;

    public OpenTSDBReporter(Configuration config) {
        mHostAndPort = config.getString("hostport");
    }

    public void report(List<Stat> stats) {
        if(mHostAndPort != null && !mHostAndPort.isEmpty()) {
            for(Stat stat : stats) {
                makeRequest(stat.toString());
            }
        }
    }

    private void makeRequest(String body) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL("http://" + mHostAndPort + "/api/put?details");
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accepts", "application/json");
            connection.setRequestProperty("Accept", "*/*");
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Length",
                        Integer.toString(body.getBytes().length));
            }
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            if (body != null) {
                DataOutputStream dataOutputStream = new DataOutputStream(
                    connection.getOutputStream());
                dataOutputStream.writeBytes(body);
                dataOutputStream.flush();
                dataOutputStream.close();
            }

            InputStream inputStream = connection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Map response = (Map) JSONValue.parse(reader);
            if (!response.get("failed").equals(0)) {
                throw new RuntimeException("url " + url + " with body " + body + " failed " +
                    JSONObject.toJSONString(response));
            }
        } catch (IOException ex) {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
