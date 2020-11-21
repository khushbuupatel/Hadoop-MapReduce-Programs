package edu.rmit.cosc2367.s3823274.Part4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3823274.Part3.WordCount;

/**
 * This reducer class reduces the output by combining the Short and Extralong
 * words to one reducer and other two type of words in second reducer
 */
public class WordLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(WordCount.class);

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// log that reducer task is started
		LOG.setLevel(Level.INFO);
		LOG.info("The reducer task of Khushbu Patel, s3823274");

		final IntWritable result = new IntWritable();
		int sum = 0;

		// iterate over values list to calculate the total count of each type of word
		for (IntWritable val : values) {
			sum += val.get();
		}

		// emit the pair containing wordtype and total count as the output
		result.set(sum);
		context.write(key, result);
	}
}