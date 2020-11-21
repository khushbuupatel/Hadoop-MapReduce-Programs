package edu.rmit.cosc2367.s3823274.Part1;

import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WordLengthCount {

	private static final Logger LOG = Logger.getLogger(WordLengthCount.class);

	/**
	 * This mapper class maps emits pair of form (word, 1) for every word present in
	 * the input file
	 */
	public static class WordLengthCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text wordType = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// log that mapper task is started
			LOG.setLevel(Level.INFO);
			LOG.info("The mapper task of Khushbu Patel, s3823274");

			// split the line from the input file into words
			StringTokenizer itr = new StringTokenizer(value.toString().trim());

			while (itr.hasMoreTokens()) {

				// map every word to count of 1
				wordType.set(getWordLength(itr.nextToken()));
				context.write(wordType, one);
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

	/**
	 * This reducer class reduces the output by combining the same type of words and
	 * summing their total count
	 *
	 */
	public static class WordLengthCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// log that reducer task is started
			LOG.setLevel(Level.INFO);
			LOG.info("The reducer task of Khushbu Patel, s3823274");

			// initialize sum to zero and loop through list of values to find total count
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			// write the type of word and the total count as the final output
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

		// log that the map
		LOG.setLevel(Level.INFO);
		LOG.info("Starting the Task 1 for Khushbu Patel, s3823274");

		Configuration conf = new Configuration();

		// set configuration and create a job for the mapreduce task
		Job job = Job.getInstance(conf, "Word Length Count");
		job.setJarByClass(WordLengthCount.class);

		// set mapper, combiner and reducer classes
		job.setMapperClass(WordLengthCountMapper.class);
		job.setCombinerClass(WordLengthCountReducer.class);
		job.setReducerClass(WordLengthCountReducer.class);

		// set output key class as Text to store the string containing the type of word
		job.setOutputKeyClass(Text.class);
		// set output value class as IntWritable to store the count of each word type
		job.setOutputValueClass(IntWritable.class);

		// delete output file path if already exists
		FileUtils.deleteDirectory(new File(args[1]));

		// set the input and output file path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// exit the program when the job is finished
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
