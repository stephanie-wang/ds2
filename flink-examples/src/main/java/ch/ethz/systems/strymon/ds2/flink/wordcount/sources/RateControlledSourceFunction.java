package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.*;
import java.util.*;

public class RateControlledSourceFunction
              extends RichParallelSourceFunction<Tuple2<Long,String>>
              implements ListCheckpointed<Tuple2<Long,String>>  {

    private Tuple2<Long,String> record;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;

    /** how many sentences to output per second **/
    private final int sentenceRate;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    private long startTime = 0;

    private long eventsCountSoFar = 0;

    // Counter used for assining timestamps to records
    private long count = 0;

    private final long maxEvents;

    private final long samplePeriod;

    private long recordTimestamp = 0L;

    public RateControlledSourceFunction(int rate, int size, int maxSentences, int period) {
        sentenceRate = rate;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
        maxEvents = maxSentences;
        samplePeriod = period;
    }

    @Override
    public void run(SourceContext<Tuple2<Long,String>> ctx) throws Exception {
        if (startTime == 0) {
          startTime = System.currentTimeMillis();
          recordTimestamp = startTime;
          Thread.sleep(1,0);  // 1ms
        }
        final Object lock = ctx.getCheckpointLock();

        while (running && (eventsCountSoFar < maxEvents)) {
            synchronized (lock) {
              long emitStartTime = System.currentTimeMillis();
              for (int i = 0; i < sentenceRate; i++) {
                String sentence = generator.nextSentence(sentenceSize);
                this.record = new Tuple2<Long,String>(-1L, sentence);
                count++;
                if (count == samplePeriod) {
                  long timestamp  = this.recordTimestamp +
                                    System.currentTimeMillis() - emitStartTime;
                  this.record.setField(timestamp, 0);
                  count = 0;
                }
                ctx.collect(this.record);
                eventsCountSoFar++;
              }
              long emitTime = System.currentTimeMillis() - emitStartTime;
              this.recordTimestamp += emitTime;
              if (emitTime < 1000) {
                  long rest = 1000 - emitTime;
                  Thread.sleep(rest);
                  this.recordTimestamp += rest;
              }
            }
        }
        double source_rate = ((eventsCountSoFar * 1000) / (System.currentTimeMillis() - startTime));
        System.out.println("Source rate: " + source_rate);
        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public List<Tuple2<Long,String>> snapshotState(long checkpointId, long checkpointTimestamp) {
        System.out.println("Checkpointing state...");
        // Make sure checkpointed state has a timestamp
        if (this.record.f0 == -1) {
          this.record.setField(System.currentTimeMillis(),0);
        }
        return Collections.singletonList(this.record);
    }

    @Override
    public void restoreState(List<Tuple2<Long,String>> state) {
        System.out.println("Restoring state...");
        for (Tuple2<Long,String> s : state)
            this.record = s;
            this.recordTimestamp = s.f0;
    }
}
