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

import hbparquet.hadoop.util.ContextUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.util.BlockCompressedStreamConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;
import fi.tkk.ics.hadoop.bam.BAMRecordReader;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.cli.CLIMRPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.BooleanOption;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.Timer;

public final class Summarize extends CLIMRPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		sortOpt        = new BooleanOption('s', "sort"),
		noTrustExtsOpt = new BooleanOption("no-trust-exts");

	public Summarize() {
		super("summarize", "summarize SAM or BAM for zooming", "3.1",
			"WORKDIR LEVELS INPATH", optionDescs,
			"Outputs, for each level in LEVELS, a summary file describing the "+
			"average number of alignments at various positions in the SAM or "+
			"BAM file in INPATH. The summary files are placed in parts in "+
			"WORKDIR."+
			"\n\n"+
			"LEVELS should be a comma-separated list of positive integers. "+
			"Each level is the number of alignments that are summarized into "+
			"one group.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			outputPathOpt, "output complete summary files to the directory "+
			               "PATH, removing the parts from WORKDIR"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			sortOpt, "sort created summaries by position"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			noTrustExtsOpt, "detect SAM/BAM files only by contents, never by "+
			                "file extension"));
	}

	private final Timer    t = new Timer();
	private       String[] levels;
	private       Path     wrkDir, mainSortOutputDir;
	private       String   wrkFile;
	private       boolean  sorted = false;

	private int missingArg(String s) {
		System.err.printf("summarize :: %s not given.\n", s);
		return 3;
	}

	@Override protected int run(CmdLineParser parser) {

		final List<String> args = parser.getRemainingArgs();
		switch (args.size()) {
			case 0: return missingArg("WORKDIR");
			case 1: return missingArg("LEVELS");
			case 2: return missingArg("INPATH");
			default: break;
		}
		if (!cacheAndSetProperties(parser))
			return 3;

		levels = args.get(1).split(",");
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

		wrkDir = new Path(args.get(0));
		final Path bam = new Path(args.get(2));

		final boolean sort = parser.getBoolean(sortOpt);

		final Configuration conf = getConf();

		conf.setBoolean(AnySAMInputFormat.TRUST_EXTS_PROPERTY,
		                !parser.getBoolean(noTrustExtsOpt));

		// Used by Utils.getMergeableWorkFile() to name the output files.
		wrkFile = bam.getName();
		conf.set(Utils.WORK_FILENAME_PROPERTY, wrkFile);

		conf.setStrings(SummarizeReducer.SUMMARY_LEVELS_PROP, levels);

		try {
			try {
				// There's a lot of different Paths here, and it can get a bit
				// confusing. Here's how it works:
				//
				// - outPath is the output dir for the final merged output, given
				//   with the -o parameter.
				//
				// - wrkDir is the user-given path where the outputs of the
				//   reducers go.
				//
				// - mergedTmpDir (defined further below) is $wrkDir/sort.tmp: if
				//   we are sorting, the summaries output in the first Hadoop job
				//   are merged in there.
				//
				// - mainSortOutputDir is $wrkDir/sorted.tmp: getSortOutputDir()
				//   gives a per-level/strand directory under it, which is used by
				//   doSorting() and mergeOne(). This is necessary because we
				//   cannot have multiple Hadoop jobs outputting into the same
				//   directory at the same time, as explained in the comment in
				//   sortMerged().

				// Required for path ".", for example.
				wrkDir = wrkDir.getFileSystem(conf).makeQualified(wrkDir);

				mainSortOutputDir = sort ? new Path(wrkDir, "sorted.tmp") : null;

				if (!runSummary(bam))
					return 4;
			} catch (IOException e) {
				System.err.printf("summarize :: Summarizing failed: %s\n", e);
				return 4;
			}

			Path mergedTmpDir = null;
			try {
				if (sort) {
					mergedTmpDir = new Path(wrkDir, "sort.tmp");
					mergeOutputs(mergedTmpDir);

				} else if (outPath != null)
					mergeOutputs(outPath);

			} catch (IOException e) {
				System.err.printf("summarize :: Merging failed: %s\n", e);
				return 5;
			}

			if (sort) {
				if (!doSorting(mergedTmpDir))
					return 6;

				// Reset this since SummarySort uses it.
				conf.set(Utils.WORK_FILENAME_PROPERTY, wrkFile);

				tryDelete(mergedTmpDir);

				if (outPath != null) try {
					sorted = true;
					mergeOutputs(outPath);
				} catch (IOException e) {
					System.err.printf(
						"summarize :: Merging sorted output failed: %s\n", e);
					return 7;
				} else {
					// Move the unmerged results out of the mainSortOutputDir
					// subdirectories to wrkDir.

					System.out.println(
						"summarize :: Moving outputs from temporary directories...");
					t.start();

					try {
						final FileSystem fs = wrkDir.getFileSystem(conf);
						for (String lvl : levels) {
							final FileStatus[] parts;

							try {
								parts = fs.globStatus(new Path(
									new Path(mainSortOutputDir, lvl + "[fr]"),
									"*-[0-9][0-9][0-9][0-9][0-9][0-9]"));
							} catch (IOException e) {
								System.err.printf(
									"summarize :: Couldn't move level %s results: %s",
									lvl, e);
								continue;
							}

							for (FileStatus part : parts) {
								final Path path = part.getPath();
								try {
									fs.rename(path, new Path(wrkDir, path.getName()));
								} catch (IOException e) {
									System.err.printf(
										"summarize :: Couldn't move '%s': %s", path, e);
								}
							}
						}
					} catch (IOException e) {
						System.err.printf(
							"summarize :: Moving results failed: %s", e);
					}
					System.out.printf("summarize :: Moved in %d.%03d s.\n",
			                        t.stopS(), t.fms());
				}
				tryDelete(mainSortOutputDir);
			}
		} catch (ClassNotFoundException e) { throw new RuntimeException(e); }
		  catch   (InterruptedException e) { throw new RuntimeException(e); }

		return 0;
	}

	private boolean runSummary(Path bamPath)
		throws IOException, ClassNotFoundException, InterruptedException
	{
		final Configuration conf = getConf();
		Utils.configureSampling(wrkDir, bamPath.getName(), conf);
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
		FileOutputFormat.setOutputPath(job, wrkDir);

		job.setPartitionerClass(TotalOrderPartitioner.class);

		System.out.println("summarize :: Sampling...");
		t.start();

		InputSampler.<LongWritable,Range>writePartitionFile(
			job, new InputSampler.RandomSampler<LongWritable,Range>(
				0.01, 10000, Math.max(100, reduceTasks)));

		System.out.printf("summarize :: Sampling complete in %d.%03d s.\n",
			               t.stopS(), t.fms());

		for (String lvl : levels) {
			MultipleOutputs.addNamedOutput(
				job, getSummaryName(lvl, false), SummarizeOutputFormat.class,
				NullWritable.class, Range.class);
			MultipleOutputs.addNamedOutput(
				job, getSummaryName(lvl,  true), SummarizeOutputFormat.class,
				NullWritable.class, Range.class);
		}

		job.submit();

		System.out.println("summarize :: Waiting for job completion...");
		t.start();

		if (!job.waitForCompletion(verbose)) {
			System.err.println("summarize :: Job failed.");
			return false;
		}

		System.out.printf("summarize :: Job complete in %d.%03d s.\n",
			               t.stopS(), t.fms());
		return true;
	}

	private void mergeOutputs(Path out)
		throws IOException
	{
		System.out.println("summarize :: Merging output...");
		t.start();

		final Configuration conf = getConf();

		final FileSystem srcFS = wrkDir.getFileSystem(conf);
		final FileSystem dstFS =    out.getFileSystem(conf);

		final Timer tl = new Timer();
		for (String l : levels) {
			mergeOne(l, false, out, srcFS, dstFS, tl);
			mergeOne(l,  true, out, srcFS, dstFS, tl);
		}
		System.out.printf("summarize :: Merging complete in %d.%03d s.\n",
			               t.stopS(), t.fms());
	}
	private void mergeOne(
			String lvl, boolean reverseStrand,
			Path out, FileSystem srcFS, FileSystem dstFS, Timer t)
		throws IOException
	{
		t.start();

		final char strand = reverseStrand ? 'r' : 'f';

		final OutputStream outs =
			dstFS.create(new Path(out, getFinalSummaryName(lvl, reverseStrand)));

		Utils.mergeInto(
			outs, sorted ? getSortOutputDir(lvl, strand) : wrkDir,
			"", "-" + getSummaryName(lvl, reverseStrand), getConf(), null);

		// Don't forget the BGZF terminator.
		outs.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
		outs.close();

		System.out.printf("summarize :: Merged %s%c in %d.%03d s.\n",
				            lvl, strand, t.stopS(), t.fms());
	}

	private boolean doSorting(Path inputDir)
		throws ClassNotFoundException, InterruptedException
	{
		final Configuration conf = getConf();
		final Job[] jobs = new Job[2*levels.length];

		boolean errors = false;

		for (int i = 0; i < levels.length; ++i) {
			final String lvl = levels[i];
			try {
				// Each job has to run in a separate directory because
				// FileOutputCommitter deletes the _temporary within whenever a job
				// completes - that is, not only the subdirectories in _temporary
				// that are specific to that job, but all of _temporary.
				//
				// It's easier to just give different temporary output directories
				// here than to override that behaviour.
				jobs[2*i] = SummarySort.sortOne(
					conf,
					new Path(inputDir, getFinalSummaryName(lvl, false)),
					getSortOutputDir(lvl, 'f'),
					"summarize", " for sorting " + lvl + 'f');

				jobs[2*i + 1] = SummarySort.sortOne(
					conf,
					new Path(inputDir, getFinalSummaryName(lvl,  true)),
					getSortOutputDir(lvl, 'r'),
					"summarize", " for sorting " + lvl + 'r');

			} catch (IOException e) {
				System.err.printf(
					"summarize :: Submitting sorting job %s failed: %s\n", lvl, e);
				if (i == 0)
					return false;
				else
					errors = true;
			}
		}

		System.out.println(
			"summarize :: Waiting for sorting jobs' completion...");
		t.start();

		for (int i = 0; i < jobs.length; ++i) {
			boolean success;
			try { success = jobs[i].waitForCompletion(verbose); }
			catch (IOException e) { success = false; }

			final String l = levels[i/2];
			final char   s = i%2 == 0 ? 'f' : 'r';

			if (!success) {
				System.err.printf(
					"summarize :: Sorting job for %s%c failed.\n", l, s);
				errors = true;
				continue;
			}
			System.out.printf(
				"summarize :: Sorting job for %s%c complete.\n", l, s);
		}
		if (errors)
			return false;

		System.out.printf("summarize :: Jobs complete in %d.%03d s.\n",
			               t.stopS(), t.fms());
		return true;
	}

	private String getFinalSummaryName(String lvl, boolean reverseStrand) {
		return wrkFile + "-" + getSummaryName(lvl, reverseStrand);
	}

	/*package*/ static String getSummaryName(String lvl, boolean reverseStrand)
	{
		return "summary" + lvl + (reverseStrand ? 'r' : 'f');
	}

	private Path getSortOutputDir(String level, char strand) {
		return new Path(mainSortOutputDir, level + strand);
	}

	private void tryDelete(Path path) {
		try {
			path.getFileSystem(getConf()).delete(path, true);
		} catch (IOException e) {
			System.err.printf(
				"summarize :: Warning: couldn't delete '%s': %s\n", path, e);
		}
	}
}

final class SummarizeReducer
	extends Reducer<LongWritable,Range, NullWritable,RangeCount>
{
	public static final String SUMMARY_LEVELS_PROP = "summarize.summary.levels";

	private MultipleOutputs<NullWritable,RangeCount> mos;

	// For the reverse and forward strands, respectively.
	private final List<SummaryGroup>
		summaryGroupsR = new ArrayList<SummaryGroup>(),
		summaryGroupsF = new ArrayList<SummaryGroup>();

	private final RangeCount summary = new RangeCount();

	// This is a safe initial choice: it doesn't matter whether the first actual
	// reference ID we get matches this or not, since all summaryLists are empty
	// anyway.
	private int currentReferenceID = 0;

	@Override public void setup(
			Reducer<LongWritable,Range, NullWritable,RangeCount>.Context ctx)
	{
		mos = new MultipleOutputs<NullWritable,RangeCount>(ctx);

		for (String s : ContextUtil.getConfiguration(ctx).getStrings(SUMMARY_LEVELS_PROP)) {
			int lvl = Integer.parseInt(s);
			summaryGroupsR.add(
				new SummaryGroup(lvl, Summarize.getSummaryName(s,  true)));
			summaryGroupsF.add(
				new SummaryGroup(lvl, Summarize.getSummaryName(s, false)));
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

			final List<SummaryGroup> summaryGroups =
				range.reverseStrand.get() ? summaryGroupsR : summaryGroupsF;

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
		for (SummaryGroup group : summaryGroupsR)
			if (group.count > 0)
				doSummary(group);
		for (SummaryGroup group : summaryGroupsF)
			if (group.count > 0)
				doSummary(group);
	}

	private void doSummary(SummaryGroup group)
		throws IOException, InterruptedException
	{
		// The reverseStrand flag is already represented in which group is passed
		// to this method, so there's no need to set it in summary.range.
		summary.rid.      set(currentReferenceID);
		summary.range.beg.set((int)(group.sumBeg / group.count));
		summary.range.end.set((int)(group.sumEnd / group.count));
		summary.count.    set(group.count);
		mos.write(NullWritable.get(), summary, group.outName);

		group.reset();
	}
}

final class Range implements Writable {
	public final IntWritable     beg           = new IntWritable();
	public final IntWritable     end           = new IntWritable();
	public final BooleanWritable reverseStrand = new BooleanWritable();

	public Range() {}
	public Range(int b, int e, boolean rev) {
		beg.          set(b);
		end.          set(e);
		reverseStrand.set(rev);
	}

	public int getCentreOfMass() {
		return (int)(((long)beg.get() + end.get()) / 2);
	}

	@Override public void write(DataOutput out) throws IOException {
		beg.          write(out);
		end.          write(out);
		reverseStrand.write(out);
	}
	@Override public void readFields(DataInput in) throws IOException {
		beg.          readFields(in);
		end.          readFields(in);
		reverseStrand.readFields(in);
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

	private AnySAMInputFormat baseIF = null;

	private void initBaseIF(final Configuration conf) {
		if (baseIF == null)
			baseIF = new AnySAMInputFormat(conf);
	}

	@Override protected boolean isSplitable(JobContext job, Path path) {
		initBaseIF(ContextUtil.getConfiguration(job));
		return baseIF.isSplitable(job, path);
	}
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		initBaseIF(ContextUtil.getConfiguration(job));
		return baseIF.getSplits(job);
	}

	@Override public RecordReader<LongWritable,Range>
		createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException
	{
		initBaseIF(ContextUtil.getConfiguration(ctx));

		final RecordReader<LongWritable,Range> rr =
			new SummarizeRecordReader(baseIF.createRecordReader(split, ctx));
		rr.initialize(split, ctx);
		return rr;
	}
}
final class SummarizeRecordReader extends RecordReader<LongWritable,Range> {

	private final RecordReader<LongWritable,SAMRecordWritable> baseRR;

	private final LongWritable key      = new LongWritable();
	private final List<Range>  ranges   = new ArrayList<Range>();
	private       int          rangeIdx = 0;

	public SummarizeRecordReader(
		RecordReader<LongWritable,SAMRecordWritable> rr)
	{
		baseRR = rr;
	}

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx) {}

	@Override public void close() throws IOException { baseRR.close(); }

	@Override public float getProgress()
		throws InterruptedException, IOException
	{
		return baseRR.getProgress();
	}

	@Override public LongWritable getCurrentKey  () { return key; }
	@Override public Range        getCurrentValue() {
		return ranges.get(rangeIdx);
	}

	@Override public boolean nextKeyValue()
		throws InterruptedException, IOException
	{
		if (rangeIdx+1 < ranges.size()) {
			++rangeIdx;
			key.set(key.get() >>> 32 << 32 | getCurrentValue().getCentreOfMass());
			return true;
		}

		SAMRecord rec;
		do {
			if (!baseRR.nextKeyValue())
				return false;

			rec = baseRR.getCurrentValue().get();
		} while (rec.getReadUnmappedFlag()
		      || rec.getReferenceIndex() < 0 || rec.getAlignmentStart() < 0);

		parseCIGAR(rec, rec.getReadNegativeStrandFlag());
		rangeIdx = 0;

		key.set(BAMRecordReader.getKey0(rec.getReferenceIndex(),
		                                getCurrentValue().getCentreOfMass()));
		return true;
	}

	void parseCIGAR(SAMRecord rec, boolean reverseStrand) {
		ranges.clear();
		final Cigar cigar = rec.getCigar();

		int begPos = rec.getAlignmentStart();
		int endPos = begPos;

		for (int i = 0; i < rec.getCigarLength(); ++i) {

			final CigarElement  element = cigar.getCigarElement(i);
			final CigarOperator op      = element.getOperator();
			switch (op) {
				case M:
				case EQ:
				case X:
					// Accumulate this part into the current range.
					endPos += element.getLength();
					continue;

				default: break;
			}

			if (begPos != endPos) {
				// No more consecutive fully contained parts: save the range and
				// move along.
				ranges.add(new Range(begPos, endPos-1, reverseStrand));
				begPos = endPos;
			}

			if (op.consumesReferenceBases()) {
				begPos += element.getLength();
				endPos = begPos;
			}
		}
		if (begPos != endPos)
			ranges.add(new Range(begPos, endPos-1, reverseStrand));
	}
}

final class SummarizeOutputFormat
	extends TextOutputFormat<NullWritable,RangeCount>
{
	@Override public RecordWriter<NullWritable,RangeCount> getRecordWriter(
			TaskAttemptContext ctx)
		throws IOException
	{
		Path path = getDefaultWorkFile(ctx, "");
		FileSystem fs = path.getFileSystem(ContextUtil.getConfiguration(ctx));

		final OutputStream file = fs.create(path);

		return new TextOutputFormat.LineRecordWriter<NullWritable,RangeCount>(
			new DataOutputStream(
				new FilterOutputStream(new BlockCompressedOutputStream(file, null))
				{
					@Override public void close() throws IOException {
						// Don't close the BlockCompressedOutputStream, so we don't
						// get an end-of-file sentinel.
						this.out.flush();

						// Instead, close the file stream directly.
						file.close();
					}
				}));
	}

	@Override public Path getDefaultWorkFile(TaskAttemptContext ctx, String ext)
		throws IOException
	{
		// From MultipleOutputs. If we had a later version of FileOutputFormat as
		// well, we'd use super.getOutputName().
		String summaryName =
			ContextUtil.getConfiguration(ctx).get("mapreduce.output.basename");

		// A RecordWriter is created as soon as a reduce task is started, even
		// though MultipleOutputs eventually overrides it with its own.
		//
		// To avoid creating a file called "inputfilename-null" when that
		// RecordWriter is initialized, make it a hidden file instead, like this.
		//
		// We can't use a filename we'd use later, because TextOutputFormat would
		// throw later on, as the file would already exist.
		String prefix = summaryName == null ? ".unused_" : "";

		return Utils.getMergeableWorkFile(
			super.getDefaultWorkFile(ctx, ext).getParent(),
			prefix, "-" + summaryName, ctx, ext);
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job) {}
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
