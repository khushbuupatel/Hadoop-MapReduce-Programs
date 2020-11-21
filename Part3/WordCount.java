package edu.rmit.cosc2367.s3823274.Part3;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WordCount {

	private static final Logger LOG = Logger.getLogger(WordCount.class);

	/**
	 * This mapper class maps implements the in-mapper combiner and emits the (word,
	 * count) pair to the reducer task
	 */
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		// hashmap to store word and it's count
		HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// log that the mapper task is started
			LOG.setLevel(Level.INFO);
			LOG.info("The mapper task of Khushbu Patel, s3823274");

			// split the line from the input file into words
			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {

				// set the current word as token
				String token = itr.nextToken();

				// if the word is already present in the wordCount hashmap then get the current
				// count and increment it by 1
				if (wordCount.containsKey(token)) {
					int count = (int) wordCount.get(token) + 1;
					wordCount.put(token, count);
				} else {
					// if the word is not present in the wordcount hashmap then add 1 as count
					wordCount.put(token, 1);
				}
			}
		}

		/**
		 * This method performs the clean up and emits the Text (word) and IntWritable
		 * (count) pair which is passed to the reducer task
		 */
		public void cleanup(Context context) throws IOException, InterruptedException {

			// create an iterator for the wordCount hashmap
			Iterator<Map.Entry<String, Integer>> wordCountIterator = wordCount.entrySet().iterator();

			// iterate over each word present in the wordCount hashmap
			while (wordCountIterator.hasNext()) {

				Map.Entry<String, Integer> entry = wordCountIterator.next();
				String keyVal = entry.getKey();
				Integer countVal = entry.getValue();

				// emit the word and local total count which will be input to reducer task
				context.write(new Text(keyVal), new IntWritable(countVal));
			}
		}
	}

	/**
	 * This reducer class reduces the output and generates the total count of words
	 * that start with a vowel and a consonant
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// log that the reducer task has started
			LOG.setLevel(Level.INFO);
			LOG.info("The reducer task of Khushbu Patel, s3823274");

			int sum = 0;

			// iterate over the values list to calculate the total count for each word
			for (IntWritable val : values) {
				sum += val.get();
			}

			// emit the word and it's total count to the output
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * This is the main driver method for the word length count mapreduce program
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// log that the mapreduce program for task 3 has started
		LOG.setLevel(Level.INFO);
		LOG.info("Starting the task 3 for Khushbu Patel, s3823274");

		Configuration conf = new Configuration();

		// set configuration and create a job for WordCount mapreduce program
		Job job = Job.getInstance(conf, "Word Count with In mapping Combiner");
		job.setJarByClass(WordCount.class);

		// set mapper and reducer class respectively
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// set output key class as Text to store the string containing the type of word
		job.setOutputKeyClass(Text.class);
		// set output value class as IntWritable to store the count of each word type
		job.setOutputValueClass(IntWritable.class);

		// delete the output file is already exists
		FileUtils.deleteDirectory(new File(args[1]));

		// set input and output file path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// exit the mapreduce program once the job is finished
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
