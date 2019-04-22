package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.checkpoint.*;
import java.util.*;
import java.util.Random;

public class RateControlledSourceFunction
              extends RichParallelSourceFunction<Tuple3<Long,String,Integer>>
              implements ListCheckpointed<Tuple3<Long,String,Integer>>  {

    private Tuple3<Long,String,Integer> record;

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

    private int id;

    public RateControlledSourceFunction(int rate, int size, int maxSentences, int period) {
        sentenceRate = rate;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
        maxEvents = maxSentences;
        samplePeriod = period;
    }

    @Override
    public void run(SourceContext<Tuple3<Long,String,Integer>> ctx) throws Exception {
        if (startTime == 0) {
          startTime = System.currentTimeMillis();
          recordTimestamp = startTime;
          Thread.sleep(1,0);  // 1ms
          Random rand = new Random();
          // Obtain a number between [0 - 49].
          id = rand.nextInt(1000);
        }
        final Object lock = ctx.getCheckpointLock();

        while (running && (eventsCountSoFar < maxEvents)) {
            synchronized (lock) {
              long emitStartTime = System.currentTimeMillis();
              for (int i = 0; i < sentenceRate; i++) {
                String sentence = generator.nextSentence(sentenceSize);
                this.record = new Tuple3<Long,String,Integer>(-1L, sentence, id);
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
    public List<Tuple3<Long,String,Integer>> snapshotState(long checkpointId, long checkpointTimestamp) {
        System.out.println("Checkpointing state...");
        // Make sure checkpointed state has a timestamp
        if (this.record.f1 == -1) {
          this.record.setField(System.currentTimeMillis(),0);
        }
        return Collections.singletonList(this.record);
    }

    @Override
    public void restoreState(List<Tuple3<Long,String,Integer>> state) {
        System.out.println("Restoring state...");
        for (Tuple3<Long,String,Integer> s : state){
            this.record = s;
            this.recordTimestamp = s.f0;
        }
    }
}
