package ch.ethz.systems.strymon.ds2.flink.wordcount;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummyLatencyCountingSink;
import ch.ethz.systems.strymon.ds2.flink.wordcount.sources.RateControlledSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class StatefulWordCount {

	private static final Logger logger  = LoggerFactory.getLogger(StatefulWordCount.class);

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// env.getConfig().setLatencyTrackingInterval(1000);  //1s

		final int checkpoinInterval = params.getInt("checkpoint-interval", -1);
		if (checkpoinInterval > 0){
			env.enableCheckpointing(checkpoinInterval);
		}

		final boolean disableOperatorChaining = params.getBoolean("disable-chaining", false);
		if (disableOperatorChaining) {
			System.out.println("Disabling chaining.");
			env.disableOperatorChaining();
		}

		final DataStream<Tuple2<Long,String>> text = env.addSource(
				new RateControlledSourceFunction(
						params.getInt("source-rate", 25000),
						params.getInt("sentence-size", 100),
						params.getInt("max-sentences", 10000000),
						params.getInt("sample-period", 1000)))
				.uid("sentence-source")
					.setParallelism(params.getInt("p1", 1));

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<Tuple3<Long, String, Long>> counts = text.rebalance()
				.flatMap(new Tokenizer())
				.name("Splitter FlatMap")
				.uid("flatmap")
					.setParallelism(params.getInt("p2", 1))
				.keyBy(1)
				.flatMap(new CountWords())
				.name("Count")
				.uid("count")
					.setParallelism(params.getInt("p3", 1));

		// write to dummy sink
		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		counts.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
				.setParallelism(params.getInt("p4", 1));

		// execute program
		env.execute("Stateful WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<Tuple2<Long,String>, Tuple3<Long, String, Long>> {
		private static final long serialVersionUID = 1L;
		private Long startTime = 0L;
		private int recordsSoFar = 0;
		private int counter = 0;

		@Override
		public void flatMap(Tuple2<Long,String> value, Collector<Tuple3<Long, String, Long>> out) throws Exception {
			if (startTime == 0) {
				startTime = System.currentTimeMillis();
			}
			recordsSoFar++;
			counter++;
			// normalize and split the line
			String[] tokens = value.f1.toLowerCase().split("\\W+");
			// emit the pairs
			for (int i=0; i<tokens.length; i++) {
				if (tokens[i].length() > 0) {
					out.collect(new Tuple3<>(value.f0, tokens[i], 1L));
				}
			}
			if (counter == 100000) {  // Print throughput and reset
				System.out.println("Flatmap throughput: " + ((recordsSoFar * 1000) / (System.currentTimeMillis() - startTime)));
				startTime = System.currentTimeMillis();
				counter = 0;
				recordsSoFar = 0;
			}
		}
	}

	public static final class CountWords extends RichFlatMapFunction<Tuple3<Long, String, Long>, Tuple3<Long, String, Long>> {

		private transient ReducingState<Long> count;
		private Long startTime = 0L;
		private int recordsSoFar = 0;
		private int counter = 0;

		@Override
		public void open(Configuration parameters) throws Exception {

			ReducingStateDescriptor<Long> descriptor =
					new ReducingStateDescriptor<Long>(
							"count", // the state name
							new Count(),
							BasicTypeInfo.LONG_TYPE_INFO);

			count = getRuntimeContext().getReducingState(descriptor);
		}

		@Override
		public void flatMap(Tuple3<Long, String, Long> value, Collector<Tuple3<Long, String, Long>> out) throws Exception {
			if (startTime == 0) {
				startTime = System.currentTimeMillis();
			}
			recordsSoFar++;
			counter++;
			count.add(value.f2);
			// Keep the timestamp (value.f0) of the new record
			if (value.f0 != -1){  // If there is an assigned timestamp
				Long elapsedTime = System.currentTimeMillis() - value.f0;
				out.collect(new Tuple3<>(elapsedTime, value.f1, count.get()));
			}
			if (counter == 100000) {  // Print throughput and reset
				System.out.println("Count throughput: " + ((recordsSoFar * 1000) / (System.currentTimeMillis() - startTime)));
				startTime = System.currentTimeMillis();
				counter = 0;
				recordsSoFar = 0;
			}
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}

}
