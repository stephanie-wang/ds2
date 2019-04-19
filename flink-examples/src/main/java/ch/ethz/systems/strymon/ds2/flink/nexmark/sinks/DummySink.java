/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.systems.strymon.ds2.flink.nexmark.sinks;

import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * A Sink that drops all data
 */
public class DummySink<T> extends StreamSink<T> {


    public DummySink() {
        super(new SinkFunction<Tuple3<Long, String, Long>>() {
            Random rand = new Random();
            // Obtain a number between [0 - 49].
            int n = rand.nextInt(1000);
            final String filename = "latencies-" + n + ".log";
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName));

            @Override
            public void invoke(Tuple3<Long, String, Long> value, Context ctx) throws Exception {
              fileWriter.write(value.f0);
            }
        });
    }
}
