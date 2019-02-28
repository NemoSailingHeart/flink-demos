package com.demos.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author guan
 * @since 2019/2/28
 */
public class FlinkWordCount {
	private static Logger logger = LoggerFactory.getLogger(FlinkWordCount.class);
	public static void main(String[] args) {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
		);

		// get input data from file
		String path = Objects.requireNonNull(FlinkWordCount.class.getClassLoader().getResource("wordCountSourceFile")).getPath();
		DataSet<String> textFileSource = env.readTextFile(path);
		DataSet<Tuple2<String, Integer>> sum = textFileSource.flatMap(new LineSplitter()).groupBy(0).sum(1);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
						// group by the tuple field "0" and sum up tuple field "1"
						.groupBy(0) //(i,1) (am,1) (chinese,1)
						.sum(1);

		// execute and print result
		try {
			logger.info("================textFileSource word:===============");
			sum.print();
			logger.info("================fromElementsSource word:===============");
			counts.print();
		} catch (Exception e) {
			logger.error("错误", e);
		}

	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&gt;).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
