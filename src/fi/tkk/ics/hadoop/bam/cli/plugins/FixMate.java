// Copyright (c) 2013 Aalto University
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

// File created: 2013-06-18 10:26:55

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import net.sf.picard.sam.ReservedTagConstants;
import net.sf.picard.sam.SamFileHeaderMerger;
import net.sf.picard.sam.SamPairUtil;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.BlockCompressedStreamConstants;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;
import fi.tkk.ics.hadoop.bam.KeyIgnoringAnySAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.cli.CLIMRBAMPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.SAMOutputPreparer;
import fi.tkk.ics.hadoop.bam.util.Timer;

public final class FixMate extends CLIMRBAMPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		sortOpt       = new BooleanOption('s', "sort"),
		noCombinerOpt = new BooleanOption('C', "no-combine");

	public FixMate() {
		super("fixmate", "BAM and SAM mate information fixing", "1.0",
			"WORKDIR INPATH [INPATH...]", optionDescs,
			"Merges together the BAM and SAM files (the INPATHs), while filling "+
			"in mate information, all distributed with Hadoop MapReduce. Output "+
			"parts are placed in WORKDIR in, by default, headerless and "+
			"unterminated BAM format."+
			"\n\n"+
			"When more than two primary reads with the same name exist in the "+
			"inputs, the result is unpredictable. Without using the -C option, "+
			"it is possible that multiple reads are mated to the same read.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			sortOpt, "also globally sort the result by query name"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			noCombinerOpt, "don't use a combiner; less efficient, but "+
			               "guarantees validity of results when there are "+
			               "multiple possible pairings"));
	}

	@Override protected int run(CmdLineParser parser) {
		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("fixmate :: WORKDIR not given.");
			return 3;
		}
		if (args.size() == 1) {
			System.err.println("fixmate :: INPATH not given.");
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

		// Used by getHeaderMerger. FixMateRecordReader needs it to correct the
		// reference indices when the output has a different index and
		// FixMateOutputFormat needs it to have the correct header for the output
		// records.
		conf.setStrings(INPUT_PATHS_PROP, strInputs.toArray(new String[0]));

		// Used by Utils.getMergeableWorkFile() to name the output files.
		final String intermediateOutName =
			(outPath == null ? inputs.get(0) : outPath).getName();
		conf.set(Utils.WORK_FILENAME_PROPERTY, intermediateOutName);

		final boolean globalSort = parser.getBoolean(sortOpt);
		conf.setBoolean(GLOBAL_SORT_PROP, globalSort);

		final Timer t = new Timer();
		try {
			// Required for path ".", for example.
			wrkDir = wrkDir.getFileSystem(conf).makeQualified(wrkDir);

			if (globalSort)
				Utils.configureSampling(wrkDir, intermediateOutName, conf);

			final Job job = new Job(conf);

			job.setJarByClass  (FixMate.class);
			job.setMapperClass (FixMateMapper.class);
			job.setReducerClass(FixMateReducer.class);

			if (!parser.getBoolean(noCombinerOpt))
				job.setCombinerClass(FixMateReducer.class);

			job.setOutputKeyClass  (Text.class);
			job.setOutputValueClass(SAMRecordWritable.class);

			job.setInputFormatClass (AnySAMInputFormat.class);
			job.setOutputFormatClass(FixMateOutputFormat.class);

			for (final Path in : inputs)
				FileInputFormat.addInputPath(job, in);

			FileOutputFormat.setOutputPath(job, wrkDir);

			if (globalSort) {
				job.setPartitionerClass(TotalOrderPartitioner.class);

				System.out.println("fixmate :: Sampling...");
				t.start();

				InputSampler.<LongWritable,SAMRecordWritable>writePartitionFile(
					job,
					new InputSampler.RandomSampler<LongWritable,SAMRecordWritable>(
						0.01, 10000, Math.max(100, reduceTasks)));

				System.out.printf("fixmate :: Sampling complete in %d.%03d s.\n",
				                  t.stopS(), t.fms());
			}

			job.submit();

			System.out.println("fixmate :: Waiting for job completion...");
			t.start();

			if (!job.waitForCompletion(verbose)) {
				System.err.println("fixmate :: Job failed.");
				return 4;
			}

			System.out.printf("fixmate :: Job complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

		} catch (IOException e) {
			System.err.printf("fixmate :: Hadoop error: %s\n", e);
			return 4;
		} catch (ClassNotFoundException e) { throw new RuntimeException(e); }
		  catch   (InterruptedException e) { throw new RuntimeException(e); }

		if (outPath != null) try {
			System.out.println("fixmate :: Merging output...");
			t.start();

			final FileSystem dstFS = outPath.getFileSystem(conf);

			// First, place the SAM or BAM header.

			final SAMFileHeader header = getHeaderMerger(conf).getMergedHeader();

			final OutputStream outs = dstFS.create(outPath);

			// Don't use the returned stream, because we're concatenating directly
			// and don't want to apply another layer of compression to BAM.
			new SAMOutputPreparer().prepareForRecords(outs, samFormat, header);

			// Then, the actual SAM or BAM contents.
			Utils.mergeInto(outs, wrkDir, "", "", conf, "fixmate");

			// And if BAM, the BGZF terminator.
			if (samFormat == SAMFormat.BAM)
				outs.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);

			outs.close();

			System.out.printf("fixmate :: Merging complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

		} catch (IOException e) {
			System.err.printf("fixmate :: Output merging failed: %s\n", e);
			return 5;
		}
		return 0;
	}

	private static final String
		INPUT_PATHS_PROP = "hadoopbam.fixmate.input.paths",
		GLOBAL_SORT_PROP = "hadoopbam.fixmate.globalsort";

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

		final SAMFileHeader.SortOrder order =
			conf.getBoolean(GLOBAL_SORT_PROP, false)
				? SAMFileHeader.SortOrder.queryname
				: SAMFileHeader.SortOrder.unsorted;

		return headerMerger = new SamFileHeaderMerger(order, headers, true);
	}
}

final class FixMateMapper
	extends Mapper<LongWritable,SAMRecordWritable, Text,SAMRecordWritable>
{
	@Override protected void map(
			LongWritable ignored, SAMRecordWritable wrec,
			Mapper<LongWritable,SAMRecordWritable, Text,SAMRecordWritable>.Context
				ctx)
		throws InterruptedException, IOException
	{
		final SamFileHeaderMerger headerMerger =
			FixMate.getHeaderMerger(ctx.getConfiguration());

		final SAMRecord     rec    = wrec.get();
		final SAMFileHeader header = rec.getHeader();

		// Correct the reference indices if necessary.
		if (headerMerger.hasMergedSequenceDictionary()) {
			rec.setReferenceIndex(headerMerger.getMergedSequenceIndex(
				header, rec.getReferenceIndex()));

			if (rec.getReadPairedFlag())
				rec.setMateReferenceIndex(headerMerger.getMergedSequenceIndex(
					header, rec.getMateReferenceIndex()));
		}

		// Correct the program group if necessary.
		if (headerMerger.hasProgramGroupCollisions()) {
			final String pg = (String)rec.getAttribute(
				ReservedTagConstants.PROGRAM_GROUP_ID);
			if (pg != null)
				rec.setAttribute(
					ReservedTagConstants.PROGRAM_GROUP_ID,
					headerMerger.getProgramGroupId(header, pg));
		}

		// Correct the read group if necessary.
		if (headerMerger.hasReadGroupCollisions()) {
			final String rg = (String)rec.getAttribute(
				ReservedTagConstants.READ_GROUP_ID);
			if (rg != null)
				rec.setAttribute(
					ReservedTagConstants.READ_GROUP_ID,
					headerMerger.getProgramGroupId(header, rg));
		}

		ctx.write(new Text(rec.getReadName()), wrec);
	}
}

// Because this can be used as a combiner, we output the key instead of a
// NullWritable.
final class FixMateReducer
	extends Reducer<Text,SAMRecordWritable, Text,SAMRecordWritable>
{
	private final SAMRecordWritable wrec = new SAMRecordWritable();

	@Override protected void reduce(
			Text key, Iterable<SAMRecordWritable> records,
			Reducer<Text,SAMRecordWritable, Text,SAMRecordWritable>.Context ctx)
		throws IOException, InterruptedException
	{
		// Non-primary records are simply written out, but as long as we can find
		// two primaries, pair them up.

		final SAMFileHeader header =
			FixMate.getHeaderMerger(ctx.getConfiguration()).getMergedHeader();

		final Iterator<SAMRecordWritable> it = records.iterator();

		while (it.hasNext()) {
			SAMRecordWritable a = it.next();

			if (a.get().getNotPrimaryAlignmentFlag()) {
				ctx.write(key, a);
				continue;
			}

			// Cache the record since the iterator does its own caching, meaning
			// that after another it.next() we would have a == b.
			wrec.set(a.get());
			a = wrec;

			SAMRecordWritable b = null;
			while (it.hasNext()) {
				b = it.next();
				if (!b.get().getNotPrimaryAlignmentFlag())
					break;
				ctx.write(key, b);
			}

			if (b == null) {
				// No more primaries, so just write the unpaired one as-is.
				ctx.write(key, a);
				break;
			}

			a.get().setHeader(header);
			b.get().setHeader(header);
			SamPairUtil.setMateInfo(a.get(), b.get(), header);

			ctx.write(key, a);
			ctx.write(key, b);
		}
	}
}

final class FixMateOutputFormat
	extends FileOutputFormat<Text,SAMRecordWritable>
{
	private KeyIgnoringAnySAMOutputFormat<Text> baseOF;

	private void initBaseOF(Configuration conf) {
		if (baseOF != null)
			return;

		baseOF = new KeyIgnoringAnySAMOutputFormat<Text>(conf);
	}

	@Override public RecordWriter<Text,SAMRecordWritable> getRecordWriter(
			TaskAttemptContext context)
		throws IOException
	{
		initBaseOF(context.getConfiguration());

		if (baseOF.getSAMHeader() == null)
			baseOF.setSAMHeader(FixMate.getHeaderMerger(
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
