package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.*;
import java.util.Collections;

public class RateControlledSourceFunction
              extends RichParallelSourceFunction<Tuple2<Long,String>>
              implements ListCheckpointed<Tuple2<Long,String>>  {

    private Long offset = 0L;

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

    private long count = 0;

    private final long maxEvents;

    private final long samplePeriod;

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
          Thread.sleep(1,0);  // 1ms
        }
        final Object lock = ctx.getCheckpointLock();

        while (running && (eventsCountSoFar < maxEvents)) {
            // for (int i = 0; i < sentenceRate; i++) {
            synchronized (lock) {
              String sentence = generator.nextSentence(sentenceSize);
              Tuple2 result = new Tuple2<Long,String>(-1L, sentence);
              count++;
              if (count == samplePeriod){
                result.setField(System.currentTimeMillis(), 0);
                count = 0;
              }
              ctx.collect(result);
              // }
              eventsCountSoFar++;
              // System.out.println(eventsCountSoFar);
              // System.out.println(startTime);
              // System.out.println(sentenceRate);
              // System.out.println(System.currentTimeMillis());
              while ((eventsCountSoFar * 1000) / (System.currentTimeMillis() - startTime) > sentenceRate) {
                  Thread.sleep(0,50000);  // 50us
              }
              offset += 1;
            }
            // Sleep for the rest of timeslice if needed
            // long emitTime = System.currentTimeMillis() - emitStartTime;
            // if (emitTime < 1000) {
            //     Thread.sleep(1000 - emitTime);
            // }
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
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Tuple2<Long,String>> state) {
        for (Tuple2<Long,String> s : state)
            offset = s;
    }
}
