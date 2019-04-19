package ch.ethz.systems.strymon.ds2.flink.wordcount;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummySink;
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
import java.io.IOException;

public class StatefulWordCount {

	private static final Logger logger  = LoggerFactory.getLogger(StatefulWordCount.class);

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		env.getConfig().setLatencyTrackingInterval(1000);  //1s

		final int checkpoinInterval = params.getInt("checkpoint-interval", -1);
		if (checkpoinInterval > 0){
			env.enableCheckpointing(checkpoinInterval);
		}

		final DataStream<String> text = env.addSource(
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
		counts.addSink(new SinkFunction<Tuple3<Long, String, Integer>>() {
						Random rand = new Random();
						// Obtain a number between [0 - 49].
						int n = rand.nextInt(1000);
						final String filename = "latencies-" + n + ".log";
						BufferedWriter fileWriter = new BufferedWriter(new FileWriter(filename));
					})
				.uid("dummy-sink")
				.setParallelism(params.getInt("p4", 1));

		// execute program
		env.execute("Stateful WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<String, Tuple3<Long, String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple3<Long, String, Long>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");
			Long timestamp = Long.parseLong(tokens[0]);
			// emit the pairs
			for (int i=1; i<tokens.length; i++) {
				if (tokens[i].length() > 0) {
					out.collect(new Tuple3<>(timestamp, tokens[i], 1L));
				}
			}
		}
	}

	public static final class CountWords extends RichFlatMapFunction<Tuple3<Long, String, Long>, Tuple3<Long, String, Long>> {

		private transient ReducingState<Long> count;

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
			count.add(value.f2);
			// Keep the timestamp (value.f0) of the new record
			out.collect(new Tuple3<>(value.f0, value.f1, count.get()));
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}

}
