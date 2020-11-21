package edu.rmit.cosc2367.s3823274.Part4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3823274.Part3.WordCount;

/**
 * This partitioner class directs the output of specific word type to a specific
 * reducer
 *
 */
public class WordLengthPartitioner extends Partitioner<Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(WordCount.class);

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {

		// log that reducer task is started
		LOG.setLevel(Level.INFO);
		LOG.info("The partitioner task of Khushbu Patel, s3823274");

		if (numReduceTasks == 0) {
			LOG.info("No partitioners founds");
			return 0;
		}

		// send all the words of type "Short-word" or "Extralong-word" to a single
		// reducer
		if (key.toString().equals("Short-word") || key.toString().equals("Extralong-word")) {
			return 0;
		} else {
			return 1;
		}
	}
}
