import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
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

// Probably the most important/interesting class here.
//
// Not a WritableComparable<RecordBlock> because we don't need/want it to be
// used as a key, only a value.
final class RecordBlock implements Writable {

	public static final int BLOCK_SIZE = 3;

	// The column we want; first column is N = 1
	private static final int N = 4;

	// Very TODO... this is just for demo purposes
	private LongWritable[] column4;
	private Text[] before, after;
	private int size;

	public RecordBlock() {
		column4 = new LongWritable[BLOCK_SIZE];
		before  = new Text[BLOCK_SIZE];
		after   = new Text[BLOCK_SIZE];
		size = BLOCK_SIZE;
	}

	public Text getSummary() {
		long sum = 0;
		for (int i = 0; i < size; ++i)
			sum += column4[i].get();
		return new Text(before[0].toString() + sum + after[0].toString());
	}

	@Override public void write(DataOutput out) throws IOException {
		for (int i = 0; i < size; ++i) {
			column4[i].write(out);
			before [i].write(out);
			after  [i].write(out);
		}
	}
	@Override public void readFields(DataInput in) throws IOException {
		for (int i = 0; i < size; ++i) {
			column4[i].readFields(in);
			before [i].readFields(in);
			after  [i].readFields(in);
		}
	}

	public void parseFrom(Text line, int i) {
		before[i] = new Text();
		after [i] = new Text();

		Text col = getNthColumn(line, before[i], after[i]);
		if (col == null)
			throw new RuntimeException("Missing column on line '" +line+ "'!");

		column4[i] = new LongWritable(Long.parseLong(col.toString()));
	}

	public void setLength(int l) { size = l; }

	private static Text getNthColumn(Text text, Text before, Text after) {
		int col = 0;
		int pos = 0;
		for (;;) {
			int npos = text.find("\t", pos);
			if (npos == -1)
				return null;

			if (++col == N) {
				// [pos,npos) is the Nth column.
				before.set(text.getBytes(), 0, pos);
				after .set(text.getBytes(), npos, text.getLength() - npos);
				return new Text(Arrays.copyOfRange(text.getBytes(), pos, npos));
			}
			pos = npos + 1;
		}
	}
}

public final class Summarize extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Summarize(), args));
	}

	private final List<Job> jobs = new ArrayList<Job>();

	@Override public int run(String[] args)
		throws ClassNotFoundException, IOException, InterruptedException
	{
		if (args.length < 2) {
			System.err.println(
				"Usage: " +Summarize.class+ " <output directory> file [file...]");
			return 2;
		}

		FileSystem fs = FileSystem.get(getConf());

		Path outputDir = new Path(args[0]);
		if (fs.exists(outputDir) && !fs.getFileStatus(outputDir).isDir()) {
			System.err.printf(
				"ERROR: specified output directory '%s' is not a directory!\n",
				outputDir);
			return 2;
		}

		List<Path> files = new ArrayList<Path>(args.length - 1);
		for (String file : Arrays.asList(args).subList(1, args.length))
			files.add(new Path(file));

		if (new HashSet<Path>(files).size() < files.size()) {
			System.err.println("ERROR: duplicate file names specified!");
			return 2;
		}

		for (Path file : files) if (!fs.isFile(file)) {
			System.err.printf("ERROR: file '%s' is not a file!\n", file);
			return 2;
		}

		for (Path file : files)
			submitJob(file, outputDir);

		int ret = 0;
		for (Job job : jobs)
			if (!job.waitForCompletion(true))
				ret = 1;
		return ret;
	}

	private void submitJob(Path inputFile, Path outputDir)
		throws ClassNotFoundException, IOException, InterruptedException
	{
		Configuration conf = new Configuration(getConf());

		// Used by SummarizeOutputFormat to construct the output filename
		conf.set(SummarizeOutputFormat.INPUT_FILENAME_PROP, inputFile.getName());

		Job job = new Job(conf);

		job.setJarByClass  (Summarize.class);
		job.setMapperClass (SummarizeMapper.class);
		job.setReducerClass(SummarizeReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setOutputKeyClass   (NullWritable.class);
		job.setOutputValueClass (Text.class);

		job.setInputFormatClass (SummarizeInputFormat.class);
		job.setOutputFormatClass(SummarizeOutputFormat.class);

		FileInputFormat .setInputPaths(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.submit();
		jobs.add(job);
	}
}

final class SummarizeMapper
	extends Mapper<LongWritable,RecordBlock, LongWritable,Text>
{
	@Override protected void map(
			LongWritable key, RecordBlock block,
			Mapper<LongWritable,RecordBlock, LongWritable,Text>.Context context)
		throws IOException, InterruptedException
	{
		context.write(key, block.getSummary());
	}
}

final class SummarizeReducer
	extends Reducer<LongWritable,Text, NullWritable,Text>
{
	@Override protected void reduce(
			LongWritable ignored, Iterable<Text> lines,
			Reducer<LongWritable,Text, NullWritable,Text>.Context context)
		throws IOException, InterruptedException
	{
		for (Text line : lines)
			context.write(NullWritable.get(), line);
	}
}

final class SummarizeInputFormat
	extends FileInputFormat<LongWritable,RecordBlock>
{
	@Override public RecordReader<LongWritable,RecordBlock> createRecordReader(
			InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException
	{
		RecordReader<LongWritable,RecordBlock> r =
			new SummarizeInputRecordReader();
		r.initialize(split, context);
		return r;
	}

	// Reads RecordBlock.BLOCK_SIZE using a LineRecordReader, then forms them
	// into a RecordBlock.
	//
	// Depending on the InputSplits we get from the LineRecordReader, we may end
	// up with blocks smaller than BLOCK_SIZE: at most one per InputSplit.
	final static class SummarizeInputRecordReader
		extends RecordReader<LongWritable,RecordBlock>
	{
		private final LineRecordReader lineReader = new LineRecordReader();

		private final RecordBlock value = new RecordBlock();

		// LineRecordReader nullifies the key when it runs out of lines, but we
		// oftentimes have a valid value even in that case, so we keep our own
		// key.
		private LongWritable key;

		@Override public RecordBlock  getCurrentValue() { return value; }
		@Override public LongWritable getCurrentKey  () { return key; }

		@Override public void initialize(
				InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException
		{
			lineReader.initialize(split, context);
		}

		@Override public boolean nextKeyValue() throws IOException {

			for (int i = 0; i < RecordBlock.BLOCK_SIZE; ++i) {
				boolean read = lineReader.nextKeyValue();
				if (!read) {
					if (i == 0)
						return false;
					value.setLength(i);
					break;
				}
				key = lineReader.getCurrentKey();
				value.parseFrom(lineReader.getCurrentValue(), i);
			}
			return true;
		}

		@Override public void close() throws IOException { lineReader.close(); }
		@Override public float getProgress() { return lineReader.getProgress(); }
	}
}
final class SummarizeOutputFormat extends TextOutputFormat<NullWritable,Text> {
	public static final String INPUT_FILENAME_PROP = "summarize.input.filename";

	@Override public Path getDefaultWorkFile(
			TaskAttemptContext context, String ext)
		throws IOException
	{
		String filename  = context.getConfiguration().get(INPUT_FILENAME_PROP);
		String extension = ext.isEmpty() ? ext : "." + ext;
		String id        = context.getTaskAttemptID().toString();
		return new Path(getOutputPath(context), filename + "_" + id + extension);
	}

	// Allow the output directory to exist, so that we can make multiple jobs
	// that write into it.
	@Override public void checkOutputSpecs(JobContext job)
		throws FileAlreadyExistsException, IOException
	{}
}
