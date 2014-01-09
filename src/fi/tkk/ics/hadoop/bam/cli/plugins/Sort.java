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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import static net.sf.samtools.SAMFileHeader.SortOrder;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;
import fi.tkk.ics.hadoop.bam.BAMRecordReader;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.cli.CLIMergingAnySAMOutputFormat;
import fi.tkk.ics.hadoop.bam.cli.CLIMRBAMPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader;
import fi.tkk.ics.hadoop.bam.util.Timer;

import hbparquet.hadoop.util.ContextUtil;

public final class Sort extends CLIMRBAMPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		stringencyOpt = new StringOption("validation-stringency=S");

	public Sort() {
		super("sort", "BAM and SAM sorting and merging", "4.2",
			"WORKDIR INPATH [INPATH...]", optionDescs,
			"Merges together the BAM and SAM files in the INPATHs, sorting the "+
			"result in standard coordinate order, all in a distributed fashion "+
			"using Hadoop MapReduce. Output parts are placed in WORKDIR in, by "+
			"default, headerless and unterminated BAM format.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			stringencyOpt, Utils.getStringencyOptHelp()));
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

		final SAMFileReader.ValidationStringency stringency =
			Utils.toStringency(parser.getOptionValue(stringencyOpt), "sort");
		if (stringency == null)
			return 3;

		Path wrkDir = new Path(args.get(0));

		final List<String> strInputs = args.subList(1, args.size());
		final List<Path> inputs = new ArrayList<Path>(strInputs.size());
		for (final String in : strInputs)
			inputs.add(new Path(in));

		final Configuration conf = getConf();

		Utils.setHeaderMergerSortOrder(conf, SortOrder.coordinate);
		conf.setStrings(
			Utils.HEADERMERGER_INPUTS_PROPERTY, strInputs.toArray(new String[0]));

		if (stringency != null)
			conf.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY,
			         stringency.toString());

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
			job.setOutputFormatClass(CLIMergingAnySAMOutputFormat.class);

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
			Utils.mergeSAMInto(outPath, wrkDir, "", "", samFormat, conf, "sort");
		} catch (IOException e) {
			System.err.printf("sort :: Output merging failed: %s\n", e);
			return 5;
		}
		return 0;
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
		initBaseIF(ContextUtil.getConfiguration(ctx));

		final RecordReader<LongWritable,SAMRecordWritable> rr =
			new SortRecordReader(baseIF.createRecordReader(split, ctx));
		rr.initialize(split, ctx);
		return rr;
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
}
final class SortRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private final RecordReader<LongWritable,SAMRecordWritable> baseRR;

	private Configuration conf;

	public SortRecordReader(RecordReader<LongWritable,SAMRecordWritable> rr) {
		baseRR = rr;
	}

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws InterruptedException, IOException
	{
		conf = ContextUtil.getConfiguration(ctx);
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

		final SAMRecord rec = getCurrentValue().get();

		final int ri = rec.getReferenceIndex();

		Utils.correctSAMRecordForMerging(rec, conf);

		if (rec.getReferenceIndex() != ri)
			getCurrentKey().set(BAMRecordReader.getKey(rec));

		return true;
	}
}
