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

    private final long maxEvents;

    public RateControlledSourceFunction(int rate, int size, int maxSentences) {
        sentenceRate = rate;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
        maxEvents = maxSentences;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        if (startTime == 0) {
          startTime = System.currentTimeMillis();
        }
        while (running && (eventsCountSoFar < maxEvents)) {
            // for (int i = 0; i < sentenceRate; i++) {
            String sentence = generator.nextSentence(sentenceSize);
            ctx.collect(sentence);
            // }
            eventsCountSoFar++;
            System.out.println(eventsCountSoFar);
            System.out.println(startTime);
            System.out.println(sentenceRate);
            System.out.println(System.currentTimeMillis);
            while (eventsCountSoFar / (System.currentTimeMillis() - startTime) > sentenceRate) {
                System.out.println("Waiting");
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
