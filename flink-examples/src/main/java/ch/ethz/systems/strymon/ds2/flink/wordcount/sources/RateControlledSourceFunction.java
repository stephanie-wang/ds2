package ch.ethz.systems.strymon.ds2.flink.wordcount.sources;

import ch.ethz.systems.strymon.ds2.common.RandomSentenceGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RateControlledSourceFunction extends RichParallelSourceFunction<String> {

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
        while (running && (eventsCountSoFar < maxEvents)) {
            // for (int i = 0; i < sentenceRate; i++) {
            String sentence = generator.nextSentence(sentenceSize);
            result = new Tuple2<Long,String>(-1, sentence);
            count++;
            if (count == samplePeriod){
              result.setField(System.currentTimeMillis(), 0);
              count = 0;
            }
            ctx.collect(sentence);
            // }
            eventsCountSoFar++;
            // System.out.println(eventsCountSoFar);
            // System.out.println(startTime);
            // System.out.println(sentenceRate);
            // System.out.println(System.currentTimeMillis());
            while ((eventsCountSoFar * 1000) / (System.currentTimeMillis() - startTime) > sentenceRate) {
                Thread.sleep(0,50000);  // 50us
            }
            // Sleep for the rest of timeslice if needed
            // long emitTime = System.currentTimeMillis() - emitStartTime;
            // if (emitTime < 1000) {
            //     Thread.sleep(1000 - emitTime);
            // }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
