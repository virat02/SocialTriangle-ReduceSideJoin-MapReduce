package st;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SocialTriangle extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(SocialTriangle.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey1 = new Text();
		private Text outvalue1 = new Text();
		private Text outkey2 = new Text();
		private Text outvalue2 = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			//split on comma, based on the input file format
			final StringTokenizer itr = new StringTokenizer(value.toString(),",");

			String temp1;
			String temp2;

			while (itr.hasMoreTokens()) {

				temp1 = itr.nextToken();
				outkey2.set(temp1);

				//set the tag as "l" for indicating the LHS
				//This indicates userId2 followed by userId1
				outvalue1.set("l " + temp1);

				temp2 = itr.nextToken();
				outkey1.set(temp2);

				//set the tag as "r" meaning indicating the RHS
				//THis indicates userId1 follows userId2
				outvalue2.set("r " + temp2);

//				logger.info("outkey1:" + outkey1);
//				logger.info("outvallue1:" + outvalue1);
//				logger.info("outkey2:" + outkey2);
//				logger.info("outvallue2:" + outvalue2);

				//on the LHS
				context.write(outkey1,outvalue1);

				//on the RHS
				context.write(outkey2, outvalue2);


			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> FollowedByList = new ArrayList<>();
		private ArrayList<Text> FollowingList = new ArrayList<>();

		private Text Y = new Text();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			// Clear our lists
			FollowedByList.clear();
			FollowingList.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {

				logger.info("value: " +t);

				if (t.charAt(0) == 'l') {
					FollowedByList.add(new Text(t.toString().substring(2)));
				} else if (t.charAt(0) == 'r') {
					FollowingList.add(new Text(t.toString().substring(2)));
				}
			}

			logger.info("key: " + key + " has FollowedByList:" + FollowedByList);
			logger.info("key: " + key + " has FollowingList:" + FollowingList);

			// Execute our join logic now that the lists are filled
			executeJoinLogic(key, context);
		}

		private void executeJoinLogic(final Text key, Context context) throws IOException,
				InterruptedException {

			// If both lists are not empty, join FollowedByList with FollowingList
			if (!FollowedByList.isEmpty() && !FollowingList.isEmpty()) {
				for (Text X : FollowedByList) {
					for (Text Z : FollowingList) {
						Y.set(key + "-->" + Z);
						context.write(X, Y);
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Social Triangle");
		job.setJarByClass(SocialTriangle.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "-->");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new SocialTriangle(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}