import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class Sort extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Sort(), args));
	}

	private final List<Job> jobs = new ArrayList<Job>();

	@Override public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		submitJob(args[0], args[1]);

		int ret = 0;
		for (Job job : jobs)
			if (!job.waitForCompletion(true))
				ret = 1;
		return ret;
	}

	private void submitJob(String inputFile, String outputDir) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration(getConf());

		// Used by SortOutputFormat to construct the output filename
		conf.set(SortOutputFormat.INPUT_FILENAME_PROP, new File(inputFile).getName());

		Job job = new Job(conf);

		job.setJarByClass  (Sort.class);
		job.setMapperClass (SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setOutputKeyClass   (NullWritable.class);
		job.setOutputValueClass (Text.class);

		job.setInputFormatClass (SortInputFormat.class);
		job.setOutputFormatClass(SortOutputFormat.class);

		FileInputFormat .setInputPaths(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.submit();
		jobs.add(job);
	}
}

// The identity function is fine.
final class SortMapper extends Mapper<LongWritable,Text, LongWritable,Text> {}

final class SortReducer extends Reducer<LongWritable,Text, NullWritable,Text> {
	@Override protected void reduce(LongWritable ignored, Iterable<Text> lines, Reducer<LongWritable,Text, NullWritable,Text>.Context context) throws IOException, InterruptedException {
		for (Text line : lines)
			context.write(NullWritable.get(), line);
	}
}

final class SortInputFormat extends FileInputFormat<LongWritable,Text> {
	@Override public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		RecordReader<LongWritable,Text> recReader = new SortInputRecordReader();
		recReader.initialize(split, context);
		return recReader;
	}

	// Wraps a LineRecordReader, but takes the Nth column of each line
	// (separated by tab characters), throwing an error if it can't be
	// losslessly converted to a long, and uses it as the key.
	final static class SortInputRecordReader extends RecordReader<LongWritable,Text> {
		private static final int N = 8; // The first column is N = 1

		private final LineRecordReader lineReader = new LineRecordReader();

		private LongWritable key = null;

		@Override public boolean nextKeyValue() throws IOException {
			boolean read = lineReader.nextKeyValue();
			if (!read)
				return false;

			Text keyCol = getNthColumn(lineReader.getCurrentValue());
			key = new LongWritable(Long.parseLong(keyCol.toString()));
			return true;
		}

		private Text getNthColumn(Text text) {
			int col = 0;
			int pos = 0;
			for (;;) {
				int npos = text.find("\t", pos);
				if (npos == -1)
					throw new RuntimeException("Ran out of tabs on line " +lineReader.getCurrentKey());

				if (++col == N) {
					// Grab [pos,npos).
					return new Text(Arrays.copyOfRange(text.getBytes(), pos, npos));
				}
				pos = npos + 1;
			}
		}

		@Override public LongWritable getCurrentKey  () { return key; }
		@Override public Text         getCurrentValue() { return lineReader.getCurrentValue(); }
		@Override public float        getProgress    () { return lineReader.getProgress(); }

		@Override public void initialize(InputSplit s, TaskAttemptContext c) throws IOException {
			lineReader.initialize(s, c);
		}
		@Override public void close() throws IOException {
			lineReader.close();
		}
	}
}
final class SortOutputFormat extends TextOutputFormat<NullWritable,Text> {
	public static final String INPUT_FILENAME_PROP = "sort.input.filename";

	@Override public Path getDefaultWorkFile(TaskAttemptContext context, String ext) throws IOException {
		String filename  = context.getConfiguration().get(INPUT_FILENAME_PROP);
		String extension = ext.isEmpty() ? ext : "." + ext;
		String id        = context.getTaskAttemptID().toString();
		return new Path(getOutputPath(context), filename + "_" + id + extension);
	}
}
