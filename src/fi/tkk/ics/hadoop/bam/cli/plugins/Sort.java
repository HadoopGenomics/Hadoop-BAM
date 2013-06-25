// Copyright (c) 2011 Aalto University
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

// File created: 2011-06-23 13:22:53

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import net.sf.picard.sam.ReservedTagConstants;
import net.sf.picard.sam.SamFileHeaderMerger;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.BlockCompressedStreamConstants;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;

import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;
import fi.tkk.ics.hadoop.bam.BAMRecordReader;
import fi.tkk.ics.hadoop.bam.KeyIgnoringAnySAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.cli.CLIMRBAMPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.util.SAMOutputPreparer;
import fi.tkk.ics.hadoop.bam.util.Timer;

public final class Sort extends CLIMRBAMPlugin {
	public Sort() {
		super("sort", "BAM and SAM sorting and merging", "4.1",
			"WORKDIR INPATH [INPATH...]", null,
			"Merges together the BAM and SAM files in the INPATHs, sorting the "+
			"result in standard coordinate order, all in a distributed fashion "+
			"using Hadoop MapReduce. Output parts are placed in WORKDIR in, by "+
			"default, headerless and unterminated BAM format.");
	}

	@Override protected int run(CmdLineParser parser) {
		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("sort :: WORKDIR not given.");
			return 3;
		}
		if (args.size() == 1) {
			System.err.println("sort :: INPATH not given.");
			return 3;
		}
		if (!cacheAndSetProperties(parser))
			return 3;

		Path wrkDir = new Path(args.get(0));

		final List<String> strInputs = args.subList(1, args.size());
		final List<Path> inputs = new ArrayList<Path>(strInputs.size());
		for (final String in : strInputs)
			inputs.add(new Path(in));

		final Configuration conf = getConf();

		// Used by getHeaderMerger. SortRecordReader needs it to correct the
		// reference indices when the output has a different index and
		// SortOutputFormat needs it to have the correct header for the output
		// records.
		conf.setStrings(INPUT_PATHS_PROP, strInputs.toArray(new String[0]));

		// Used by Utils.getMergeableWorkFile() to name the output files.
		final String intermediateOutName =
			(outPath == null ? inputs.get(0) : outPath).getName();
		conf.set(Utils.WORK_FILENAME_PROPERTY, intermediateOutName);

		final Timer t = new Timer();
		try {
			// Required for path ".", for example.
			wrkDir = wrkDir.getFileSystem(conf).makeQualified(wrkDir);

			Utils.configureSampling(wrkDir, intermediateOutName, conf);

			final Job job = new Job(conf);

			job.setJarByClass  (Sort.class);
			job.setMapperClass (Mapper.class);
			job.setReducerClass(SortReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setOutputKeyClass   (NullWritable.class);
			job.setOutputValueClass (SAMRecordWritable.class);

			job.setInputFormatClass (SortInputFormat.class);
			job.setOutputFormatClass(SortOutputFormat.class);

			for (final Path in : inputs)
				FileInputFormat.addInputPath(job, in);

			FileOutputFormat.setOutputPath(job, wrkDir);

			job.setPartitionerClass(TotalOrderPartitioner.class);

			System.out.println("sort :: Sampling...");
			t.start();

			InputSampler.<LongWritable,SAMRecordWritable>writePartitionFile(
				job,
				new InputSampler.RandomSampler<LongWritable,SAMRecordWritable>(
					0.01, 10000, Math.max(100, reduceTasks)));

			System.out.printf("sort :: Sampling complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

			job.submit();

			System.out.println("sort :: Waiting for job completion...");
			t.start();

			if (!job.waitForCompletion(verbose)) {
				System.err.println("sort :: Job failed.");
				return 4;
			}

			System.out.printf("sort :: Job complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

		} catch (IOException e) {
			System.err.printf("sort :: Hadoop error: %s\n", e);
			return 4;
		} catch (ClassNotFoundException e) { throw new RuntimeException(e); }
		  catch   (InterruptedException e) { throw new RuntimeException(e); }

		if (outPath != null) try {
			System.out.println("sort :: Merging output...");
			t.start();

			final FileSystem dstFS = outPath.getFileSystem(conf);

			// First, place the BAM header.

			final SAMFileHeader header = getHeaderMerger(conf).getMergedHeader();

			final OutputStream outs = dstFS.create(outPath);

			// Don't use the returned stream, because we're concatenating directly
			// and don't want to apply another layer of compression to BAM.
			new SAMOutputPreparer().prepareForRecords(outs, samFormat, header);

			// Then, the actual SAM or BAM contents.
			Utils.mergeInto(outs, wrkDir, "", "", conf, "sort");

			// And if BAM, the BGZF terminator.
			if (samFormat == SAMFormat.BAM)
				outs.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);

			outs.close();

			System.out.printf("sort :: Merging complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

		} catch (IOException e) {
			System.err.printf("sort :: Output merging failed: %s\n", e);
			return 5;
		}
		return 0;
	}

	private static final String INPUT_PATHS_PROP = "hadoopbam.sort.input.paths";

	private static SamFileHeaderMerger headerMerger = null;

	public static SamFileHeaderMerger getHeaderMerger(Configuration conf)
		throws IOException
	{
		// TODO: it would be preferable to cache this beforehand instead of
		// having every task read the header block of every input file. But that
		// would be trickier, given that SamFileHeaderMerger isn't trivially
		// serializable.

		// Save it in a static field, though, in case that helps anything.
		if (headerMerger != null)
			return headerMerger;

		final List<SAMFileHeader> headers = new ArrayList<SAMFileHeader>();

		for (final String in : conf.getStrings(INPUT_PATHS_PROP)) {
			final Path p = new Path(in);

			final SAMFileReader r =
				new SAMFileReader(p.getFileSystem(conf).open(p));
			headers.add(r.getFileHeader());
			r.close();
		}

		return headerMerger = new SamFileHeaderMerger(
			SAMFileHeader.SortOrder.coordinate, headers, true);
	}
}

final class SortReducer
	extends Reducer<LongWritable,SAMRecordWritable,
	                NullWritable,SAMRecordWritable>
{
	@Override protected void reduce(
			LongWritable ignored, Iterable<SAMRecordWritable> records,
			Reducer<LongWritable,SAMRecordWritable,
			        NullWritable,SAMRecordWritable>.Context
				ctx)
		throws IOException, InterruptedException
	{
		for (SAMRecordWritable rec : records)
			ctx.write(NullWritable.get(), rec);
	}
}

// Because we want a total order and we may change the key when merging
// headers, we can't use a mapper here: the InputSampler reads directly from
// the InputFormat.
final class SortInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	private AnySAMInputFormat baseIF = null;

	private void initBaseIF(final Configuration conf) {
		if (baseIF == null)
			baseIF = new AnySAMInputFormat(conf);
	}

	@Override public RecordReader<LongWritable,SAMRecordWritable>
		createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException
	{
		initBaseIF(ctx.getConfiguration());

		final RecordReader<LongWritable,SAMRecordWritable> rr =
			new SortRecordReader(baseIF.createRecordReader(split, ctx));
		rr.initialize(split, ctx);
		return rr;
	}

	@Override protected boolean isSplitable(JobContext job, Path path) {
		initBaseIF(job.getConfiguration());
		return baseIF.isSplitable(job, path);
	}
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		initBaseIF(job.getConfiguration());
		return baseIF.getSplits(job);
	}
}
final class SortRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private final RecordReader<LongWritable,SAMRecordWritable> baseRR;

	private SamFileHeaderMerger headerMerger;

	public SortRecordReader(RecordReader<LongWritable,SAMRecordWritable> rr) {
		baseRR = rr;
	}

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws InterruptedException, IOException
	{
		headerMerger = Sort.getHeaderMerger(ctx.getConfiguration());
	}

	@Override public void close() throws IOException { baseRR.close(); }

	@Override public float getProgress()
		throws InterruptedException, IOException
	{
		return baseRR.getProgress();
	}

	@Override public LongWritable getCurrentKey()
		throws InterruptedException, IOException
	{
		return baseRR.getCurrentKey();
	}
	@Override public SAMRecordWritable getCurrentValue()
		throws InterruptedException, IOException
	{
		return baseRR.getCurrentValue();
	}

	@Override public boolean nextKeyValue()
		throws InterruptedException, IOException
	{
		if (!baseRR.nextKeyValue())
			return false;

		final SAMRecord     r = getCurrentValue().get();
		final SAMFileHeader h = r.getHeader();

		// Correct the reference indices, and thus the key, if necessary.
		if (headerMerger.hasMergedSequenceDictionary()) {
			int ri = headerMerger.getMergedSequenceIndex(
				h, r.getReferenceIndex());

			r.setReferenceIndex(ri);
			if (r.getReadPairedFlag())
				r.setMateReferenceIndex(headerMerger.getMergedSequenceIndex(
					h, r.getMateReferenceIndex()));

			getCurrentKey().set(BAMRecordReader.getKey(r));
		}

		// Correct the program group if necessary.
		if (headerMerger.hasProgramGroupCollisions()) {
			final String pg = (String)r.getAttribute(
				ReservedTagConstants.PROGRAM_GROUP_ID);
			if (pg != null)
				r.setAttribute(
					ReservedTagConstants.PROGRAM_GROUP_ID,
					headerMerger.getProgramGroupId(h, pg));
		}

		// Correct the read group if necessary.
		if (headerMerger.hasReadGroupCollisions()) {
			final String rg = (String)r.getAttribute(
				ReservedTagConstants.READ_GROUP_ID);
			if (rg != null)
				r.setAttribute(
					ReservedTagConstants.READ_GROUP_ID,
					headerMerger.getProgramGroupId(h, rg));
		}

		return true;
	}
}

final class SortOutputFormat
	extends FileOutputFormat<NullWritable,SAMRecordWritable>
{
	private KeyIgnoringAnySAMOutputFormat<NullWritable> baseOF;

	private void initBaseOF(Configuration conf) {
		if (baseOF != null)
			return;

		baseOF = new KeyIgnoringAnySAMOutputFormat<NullWritable>(conf);
	}

	@Override
	public RecordWriter<NullWritable,SAMRecordWritable> getRecordWriter(
			TaskAttemptContext context)
		throws IOException
	{
		initBaseOF(context.getConfiguration());

		if (baseOF.getSAMHeader() == null)
			baseOF.setSAMHeader(Sort.getHeaderMerger(
				context.getConfiguration()).getMergedHeader());

		return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
	}

	@Override public Path getDefaultWorkFile(TaskAttemptContext ctx, String ext)
		throws IOException
	{
		initBaseOF(ctx.getConfiguration());
		return Utils.getMergeableWorkFile(
			baseOF.getDefaultWorkFile(ctx, ext).getParent(), "", "", ctx, ext);
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job) {}
}
