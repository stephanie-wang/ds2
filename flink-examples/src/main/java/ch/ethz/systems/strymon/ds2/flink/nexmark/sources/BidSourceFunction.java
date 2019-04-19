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

package ch.ethz.systems.strymon.ds2.flink.nexmark.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import org.slf4j.Logger;

/**
 * A ParallelSourceFunction that generates Nexmark Bid data
 */
public class BidSourceFunction extends RichParallelSourceFunction<Bid> {

    private volatile boolean running = true;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private long eventsCountSoFar = 0;
    private final int rate;
    private final int maxEvents;
    private final Logger logger;
    private long startTime = 0;

    public BidSourceFunction(Logger log, int srcRate, int maxEvents) {
        this.rate = srcRate;
        if (maxEvents == 0) {
            maxEvents = 20_000_000;
        }
        this.maxEvents = maxEvents;
        this.logger = log;
    }

    @Override
    public void run(SourceContext<Bid> ctx) throws Exception {
        if (startTime == 0) {
          startTime = System.currentTimeMillis();
        }
        while (running && (eventsCountSoFar < maxEvents)) {
            long emitStartTime = System.currentTimeMillis();

            // for (int i = 0; i < rate; i++) {

            long nextId = nextId();
            Random rnd = new Random(nextId);

            // When, in event time, we should generate the event. Monotonic.
            long eventTimestamp =
                      config.timestampAndInterEventDelayUsForEvent(
                          config.nextEventNumber(eventsCountSoFar)).getKey();

            ctx.collect(BidGenerator.nextBid(nextId, rnd, eventTimestamp, config));

            eventsCountSoFar++;

            while ((eventsCountSoFar * 1000)  / (System.currentTimeMillis() - startTime) > rate) {
                Thread.sleep(0,50000);  // 50us
            }
            // }

            // Sleep for the rest of timeslice if needed
            logger.warn("Bid throughput: {}", eventsCountSoFar / (System.currentTimeMillis() - startTime) * 1000);
            // long emitTime = System.currentTimeMillis() - emitStartTime;
            // if (emitTime < 1000) {
            //     Thread.sleep(1000 - emitTime);
            // }
        }
        long finishTime = System.currentTimeMillis();
        logger.warn("Bid THROUGHPUT: {}", eventsCountSoFar / (finishTime - startTime) * 1000);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }

}
