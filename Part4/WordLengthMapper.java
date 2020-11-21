package edu.rmit.cosc2367.s3823274.Part4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3823274.Part3.WordCount;

public class WordLengthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(WordCount.class);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// log that the mapper task has started
		LOG.setLevel(Level.INFO);
		LOG.info("The mapper task of Khushbu Patel, s3823274");

		try {

			// split the line from the input file into words
			StringTokenizer itr = new StringTokenizer(value.toString().trim());

			while (itr.hasMoreTokens()) {

				// emit pair containing the word type and the count as 1 to partitioner task
				context.write(new Text(getWordLength(itr.nextToken())), new IntWritable(1));
			}

		} catch (Exception ex) {
			LOG.error("The mapper task of Khushbu Patel, s3823274");
		}
	}

	/**
	 * This method returns the type of given word (Short-word, Medium-word,
	 * Long-word, Extralong-word) based on its length of the given word
	 * 
	 * @param word
	 * @return type of word
	 */
	public String getWordLength(String word) {
		if (word.length() >= 1 && word.length() <= 4) {
			return "Short-word";
		} else if (word.length() >= 5 && word.length() <= 7) {
			return "Medium-word";
		} else if (word.length() >= 8 && word.length() <= 10) {
			return "Long-word";
		} else {
			return "Extralong-word";
		}
	}
}