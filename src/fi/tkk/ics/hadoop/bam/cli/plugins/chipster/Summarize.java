// Copyright (c) 2010 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package fi.tkk.ics.hadoop.bam.cli.plugins.chipster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import hadooptrunk.MultipleOutputs;

import net.sf.samtools.util.BlockCompressedStreamConstants;

import fi.tkk.ics.hadoop.bam.custom.hadoop.InputSampler;
import fi.tkk.ics.hadoop.bam.custom.hadoop.TotalOrderPartitioner;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import fi.tkk.ics.hadoop.bam.custom.samtools.BlockCompressedOutputStream;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;

import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.BAMRecordReader;
import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.util.Pair;

public final class Summarize extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		outputDirOpt      = new  StringOption('o', "output-dir=PATH"),
		outputLocalDirOpt = new  StringOption('O', "output-local-dir=PATH");

	public Summarize() {
		super("summarize", "summarize BAM for zooming", "1.0",
			"WORKDIR LEVELS PATH", optionDescs,
			"Outputs, for each level in LEVELS, a summary file describing the "+
			"average number of alignments at various positions in the BAM file "+
			"in PATH. The summary files are placed in parts in WORKDIR."+
			"\n\n"+
			"LEVELS should be a comma-separated list of positive integers. "+
			"Each level is the number of alignments that are summarized into "+
			"one group.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			outputDirOpt, "output complete summary files to the file PATH, "+
			              "removing the parts from WORKDIR"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			outputLocalDirOpt, "like -o, but treat PATH as referring to the "+
			                   "local FS"));
	}

	private int missingArg(String s) {
		System.err.printf("summarize :: %s not given.\n", s);
		return 3;
	}

	@Override protected int run(CmdLineParser parser) {

		final List<String> args = parser.getRemainingArgs();
		switch (args.size()) {
			case 0: return missingArg("WORKDIR");
			case 1: return missingArg("LEVELS");
			case 2: return missingArg("PATH");
			default: break;
		}

		final String wrkDir  = args.get(0),
		             bam     = args.get(2),
		             outAny  = (String)parser.getOptionValue(outputDirOpt),
		             outLoc  = (String)parser.getOptionValue(outputLocalDirOpt),
		             out;

		if (outAny != null) {
			if (outLoc != null) {
				System.err.println("summarize :: cannot accept both -o and -O!");
				return 3;
			}
			out = outAny;
		} else
			out = outLoc;

		final String[] levels = args.get(1).split(",");
		for (String l : levels) {
			try {
				int lvl = Integer.parseInt(l);
				if (lvl > 0)
					continue;
				System.err.printf(
					"summarize :: summary level '%d' is not positive!\n", lvl);
			} catch (NumberFormatException e) {
				System.err.printf(
					"summarize :: summary level '%s' is not an integer!\n", l);
			}
			return 3;
		}

		final Path   bamPath    = new Path(bam),
		             wrkDirPath = new Path(wrkDir);
		final String bamFile    = bamPath.getName();

		final Configuration conf = getConf();

		// Used by SummarizeOutputFormat to name the output files.
		conf.set(SummarizeOutputFormat.OUTPUT_NAME_PROP, bamFile);

		conf.setStrings(SummarizeReducer.SUMMARY_LEVELS_PROP, levels);

		try {
			setSamplingConf(bamPath.getParent(), bamFile, conf);

			// As far as I can tell there's no non-deprecated way of getting this
			// info. We can silence this warning but not the import.
			@SuppressWarnings("deprecation")
			int maxReduceTasks =
				new JobClient(new JobConf(conf)).getClusterStatus()
				.getMaxReduceTasks();

			conf.setInt("mapred.reduce.tasks", Math.max(1, maxReduceTasks*9/10));

			final Job job = new Job(conf);

			job.setJarByClass  (Summarize.class);
			job.setMapperClass (Mapper.class);
			job.setReducerClass(SummarizeReducer.class);

			job.setMapOutputKeyClass  (LongWritable.class);
			job.setMapOutputValueClass(Range.class);
			job.setOutputKeyClass     (NullWritable.class);
			job.setOutputValueClass   (RangeCount.class);

			job.setInputFormatClass (SummarizeInputFormat.class);
			job.setOutputFormatClass(SummarizeOutputFormat.class);

			FileInputFormat .setInputPaths(job, bamPath);
			FileOutputFormat.setOutputPath(job, wrkDirPath);

			job.setPartitionerClass(TotalOrderPartitioner.class);

			System.out.println("summarize :: Sampling...");

			InputSampler.<LongWritable,Range>writePartitionFile(
				job,
				new InputSampler.SplitSampler<LongWritable,Range>(
					1 << 16, 10));

			System.out.println("summarize :: Sampling complete.");

			for (String lvl : levels)
				MultipleOutputs.addNamedOutput(
					job, "summary" + lvl, SummarizeOutputFormat.class,
					NullWritable.class, Range.class);

			job.submit();

			if (!job.waitForCompletion(true))
				return 4;
		} catch (IOException e) {
			System.err.printf("summarize :: Hadoop error: %s\n", e);
			return 4;
		} catch (ClassNotFoundException e) { throw new RuntimeException(e); }
		  catch   (InterruptedException e) { throw new RuntimeException(e); }

		if (out != null) try {
			final Path outPath = new Path(out);

			final FileSystem srcFS = wrkDirPath.getFileSystem(conf);
			      FileSystem dstFS =    outPath.getFileSystem(conf);

			if (out == outLoc)
				dstFS = FileSystem.getLocal(conf).getRaw();

			final String baseName =
				conf.get(SummarizeOutputFormat.OUTPUT_NAME_PROP);

			for (String lvl : levels) {
				final String lvlName = baseName + "-summary" + lvl;

				final OutputStream outs = dstFS.create(new Path(out, lvlName));

				final FileStatus[] parts = srcFS.globStatus(new Path(
					wrkDir, lvlName + "-[0-9][0-9][0-9][0-9][0-9][0-9]*"));

				for (final FileStatus part : parts) {
					final InputStream ins = srcFS.open(part.getPath());
					IOUtils.copyBytes(ins, outs, conf, false);
					ins.close();
				}
				for (final FileStatus part : parts)
					srcFS.delete(part.getPath(), false);

				// Don't forget the BGZF terminator.
				outs.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
				outs.close();
			}
		} catch (IOException e) {
			System.err.printf("summarize :: output combining failed: %s\n", e);
			return 5;
		}
		return 0;
	}

	private void setSamplingConf(
			Path inputDir, String inputName, Configuration conf)
		throws IOException
	{
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));

		Path partition = new Path(inputDir, "_partitioning" + inputName);
		TotalOrderPartitioner.setPartitionFile(conf, partition);

		try {
			URI partitionURI = new URI(
				partition.toString() + "#_partitioning" + inputName);
			DistributedCache.addCacheFile(partitionURI, conf);
			DistributedCache.createSymlink(conf);
		} catch (URISyntaxException e) { throw new RuntimeException(e); }
	}
}

final class SummarizeReducer
	extends Reducer<LongWritable,Range, NullWritable,RangeCount>
{
	public static final String SUMMARY_LEVELS_PROP = "summarize.summary.levels";

	private MultipleOutputs<NullWritable,RangeCount> mos;

	private final List<SummaryGroup> summaryGroups =
		new ArrayList<SummaryGroup>();

	private final RangeCount summary = new RangeCount();

	// This is a safe initial choice: it doesn't matter whether the first actual
	// reference ID we get matches this or not, since all summaryLists are empty
	// anyway.
	private int currentReferenceID = 0;

	@Override public void setup(
			Reducer<LongWritable,Range, NullWritable,RangeCount>.Context ctx)
	{
		mos = new MultipleOutputs<NullWritable,RangeCount>(ctx);

		for (String s : ctx.getConfiguration().getStrings(SUMMARY_LEVELS_PROP)) {
			int level = Integer.parseInt(s);
			summaryGroups.add(new SummaryGroup(level, "summary" + level));
		}
	}

	@Override protected void reduce(
			LongWritable key, Iterable<Range> ranges,
			Reducer<LongWritable,Range, NullWritable,RangeCount>.Context context)
		throws IOException, InterruptedException
	{
		final int referenceID = (int)(key.get() >>> 32);

		// When the reference sequence changes we have to flush out everything
		// we've got and start from scratch again.
		if (referenceID != currentReferenceID) {
			currentReferenceID = referenceID;
			doAllSummaries();
		}

		for (final Range range : ranges) {
			final int beg = range.beg.get(),
			          end = range.end.get();

			for (SummaryGroup group : summaryGroups) {
				group.sumBeg += beg;
				group.sumEnd += end;
				if (++group.count == group.level)
					doSummary(group);
			}
		}
	}

	@Override protected void cleanup(
			Reducer<LongWritable,Range, NullWritable,RangeCount>.Context context)
		throws IOException, InterruptedException
	{
		// Don't lose any remaining ones at the end.
		doAllSummaries();

		mos.close();
	}

	private void doAllSummaries() throws IOException, InterruptedException {
		for (SummaryGroup group : summaryGroups)
			if (group.count > 0)
				doSummary(group);
	}

	private void doSummary(SummaryGroup group)
		throws IOException, InterruptedException
	{
		summary.rid.      set(currentReferenceID);
		summary.range.beg.set((int)(group.sumBeg / group.count));
		summary.range.end.set((int)(group.sumEnd / group.count));
		summary.count.    set(group.count);
		mos.write(NullWritable.get(), summary, group.outName);

		group.reset();
	}
}

final class Range implements Writable {
	public final IntWritable beg = new IntWritable();
	public final IntWritable end = new IntWritable();

	public void setFrom(SAMRecord record) {
		beg.set(record.getAlignmentStart());
		end.set(record.getAlignmentEnd());
	}

	public int getCentreOfMass() {
		return (int)(((long)beg.get() + end.get()) / 2);
	}

	@Override public void write(DataOutput out) throws IOException {
		beg.write(out);
		end.write(out);
	}
	@Override public void readFields(DataInput in) throws IOException {
		beg.readFields(in);
		end.readFields(in);
	}
}

final class RangeCount implements Comparable<RangeCount>, Writable {
	public final Range       range = new Range();
	public final IntWritable count = new IntWritable();
	public final IntWritable rid   = new IntWritable();

	// This is what the TextOutputFormat will write. The format is
	// tabix-compatible; see http://samtools.sourceforge.net/tabix.shtml.
	//
	// It might not be sorted by range.beg though! With the centre of mass
	// approach, it most likely won't be.
	@Override public String toString() {
		return rid
		     + "\t" + range.beg
		     + "\t" + range.end
		     + "\t" + count;
	}

	// Comparisons only take into account the leftmost position.
	@Override public int compareTo(RangeCount o) {
		return Integer.valueOf(range.beg.get()).compareTo(o.range.beg.get());
	}

	@Override public void write(DataOutput out) throws IOException {
		range.write(out);
		count.write(out);
		rid  .write(out);
	}
	@Override public void readFields(DataInput in) throws IOException {
		range.readFields(in);
		count.readFields(in);
		rid  .readFields(in);
	}
}

// We want the centre of mass to be used as (the low order bits of) the key
// already at this point, because we want a total order so that we can
// meaningfully look at consecutive ranges in the reducers. If we were to set
// the final key in the mapper, the partitioner wouldn't use it.
//
// And since getting the centre of mass requires calculating the Range as well,
// we might as well get that here as well.
final class SummarizeInputFormat extends FileInputFormat<LongWritable,Range> {

	private final BAMInputFormat bamIF = new BAMInputFormat();

	@Override protected boolean isSplitable(JobContext job, Path path) {
		return bamIF.isSplitable(job, path);
	}
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		return bamIF.getSplits(job);
	}

	@Override public RecordReader<LongWritable,Range>
		createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException
	{
		final RecordReader<LongWritable,Range> rr = new SummarizeRecordReader();
		rr.initialize(split, ctx);
		return rr;
	}
}
final class SummarizeRecordReader extends RecordReader<LongWritable,Range> {

	private final BAMRecordReader bamRR = new BAMRecordReader();
	private final LongWritable    key   = new LongWritable();
	private final Range           range = new Range();

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		bamRR.initialize(spl, ctx);
	}
	@Override public void close() throws IOException { bamRR.close(); }

	@Override public float getProgress() { return bamRR.getProgress(); }

	@Override public LongWritable getCurrentKey  () { return key; }
	@Override public Range        getCurrentValue() { return range; }

	@Override public boolean nextKeyValue() {
		SAMRecord rec;

		do {
			if (!bamRR.nextKeyValue())
				return false;

			rec = bamRR.getCurrentValue().get();
		} while (rec.getReadUnmappedFlag());

		range.setFrom(rec);
		key.set((long)rec.getReferenceIndex() << 32 | range.getCentreOfMass());
		return true;
	}
}

final class SummarizeOutputFormat
	extends TextOutputFormat<NullWritable,RangeCount>
{
	public static final String OUTPUT_NAME_PROP =
		"hadoopbam.summarize.output.name";

	@Override public RecordWriter<NullWritable,RangeCount> getRecordWriter(
			TaskAttemptContext ctx)
		throws IOException
	{
		Path path = getDefaultWorkFile(ctx, "");
		FileSystem fs = path.getFileSystem(ctx.getConfiguration());

		return new TextOutputFormat.LineRecordWriter<NullWritable,RangeCount>(
			new DataOutputStream(
				new BlockCompressedOutputStream(fs.create(path))));
	}

	@Override public Path getDefaultWorkFile(
			TaskAttemptContext context, String ext)
		throws IOException
	{
		Configuration conf = context.getConfiguration();

		// From MultipleOutputs. If we had a later version of FileOutputFormat as
		// well, we'd use getOutputName().
		String summaryName = conf.get("mapreduce.output.basename");

		// A RecordWriter is created as soon as a reduce task is started, even
		// though MultipleOutputs eventually overrides it with its own.
		//
		// To avoid creating a file called "inputfilename-null" when that
		// RecordWriter is initialized, make it a hidden file instead, like this.
		//
		// We can't use a filename we'd use later, because TextOutputFormat would
		// throw later on, as the file would already exist.
		String baseName = summaryName == null ? ".unused_" : "";

		baseName         += conf.get(OUTPUT_NAME_PROP);
		String extension  = ext.isEmpty() ? ext : "." + ext;
		int    part       = context.getTaskAttemptID().getTaskID().getId();
		return new Path(super.getDefaultWorkFile(context, ext).getParent(),
			  baseName    + "-"
			+ summaryName + "-"
			+ String.format("%06d", part)
			+ extension);
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job)
		throws FileAlreadyExistsException, IOException
	{}
}

final class SummaryGroup {
	public       int    count;
	public final int    level;
	public       long   sumBeg, sumEnd;
	public final String outName;

	public SummaryGroup(int lvl, String name) {
		level   = lvl;
		outName = name;
		reset();
	}

	public void reset() {
		sumBeg = sumEnd = 0;
		count  = 0;
	}
}
