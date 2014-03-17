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

import hbparquet.hadoop.util.ContextUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.picard.sam.SamPairUtil;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.cli.CLIMRBAMPlugin;
import fi.tkk.ics.hadoop.bam.cli.CLIMergingAnySAMOutputFormat;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.BooleanOption;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.StringOption;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader;
import fi.tkk.ics.hadoop.bam.util.Timer;

public final class FixMate extends CLIMRBAMPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		sortOpt       = new BooleanOption('s', "sort"),
		noCombinerOpt = new BooleanOption('C', "no-combine"),
		stringencyOpt = new  StringOption("validation-stringency=S");

	public FixMate() {
		super("fixmate", "BAM and SAM mate information fixing", "1.1",
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
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			stringencyOpt, Utils.getStringencyOptHelp()));
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

		final SAMFileReader.ValidationStringency stringency =
			Utils.toStringency(parser.getOptionValue(stringencyOpt, SAMFileReader.ValidationStringency.DEFAULT_STRINGENCY.toString()), "fixmate");
		if (stringency == null)
			return 3;

		Path wrkDir = new Path(args.get(0));

		final List<String> strInputs = args.subList(1, args.size());
		final List<Path> inputs = new ArrayList<Path>(strInputs.size());
		for (final String in : strInputs)
			inputs.add(new Path(in));

		final Configuration conf = getConf();

		// Used by Utils.getMergeableWorkFile() to name the output files.
		final String intermediateOutName =
			(outPath == null ? inputs.get(0) : outPath).getName();
		conf.set(Utils.WORK_FILENAME_PROPERTY, intermediateOutName);

		if (stringency != null)
			conf.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY,
			         stringency.toString());

		final boolean globalSort = parser.getBoolean(sortOpt);
		if (globalSort)
			Utils.setHeaderMergerSortOrder(
				conf, SAMFileHeader.SortOrder.queryname);

		conf.setStrings(
			Utils.HEADERMERGER_INPUTS_PROPERTY, strInputs.toArray(new String[0]));

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
			job.setOutputFormatClass(CLIMergingAnySAMOutputFormat.class);

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
			Utils.mergeSAMInto(outPath, wrkDir,"","", samFormat, conf, "fixmate");
		} catch (IOException e) {
			System.err.printf("fixmate :: Output merging failed: %s\n", e);
			return 5;
		}
		return 0;
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
		Utils.correctSAMRecordForMerging(wrec.get(), ContextUtil.getConfiguration(ctx));
		ctx.write(new Text(wrec.get().getReadName()), wrec);
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
			Utils.getSAMHeaderMerger(ContextUtil.getConfiguration(ctx)).getMergedHeader();

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
