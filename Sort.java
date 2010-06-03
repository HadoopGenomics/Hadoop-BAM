import java.io.IOException;
import java.util.Arrays;

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

	@Override public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Job job = new Job(getConf());

		job.setJarByClass  (Sort.class);
		job.setMapperClass (SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setOutputKeyClass   (NullWritable.class);
		job.setOutputValueClass (Text.class);

		job.setInputFormatClass (SortInputFormat.class);
		job.setOutputFormatClass(SortOutputFormat.class);

		FileInputFormat .setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
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
final class SortOutputFormat extends TextOutputFormat<NullWritable,Text> {}
