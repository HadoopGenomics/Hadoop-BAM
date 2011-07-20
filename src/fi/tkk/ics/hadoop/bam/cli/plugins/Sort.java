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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import net.sf.samtools.util.BlockCompressedStreamConstants;

import fi.tkk.ics.hadoop.bam.custom.hadoop.InputSampler;
import fi.tkk.ics.hadoop.bam.custom.hadoop.TotalOrderPartitioner;
import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import fi.tkk.ics.hadoop.bam.custom.samtools.BAMFileWriter;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileHeader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;

import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.Timer;

public final class Sort extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		verboseOpt    = new BooleanOption('v', "verbose"),
		outputFileOpt = new  StringOption('o', "output-file=PATH");

	public Sort() {
		super("sort", "BAM sorting", "1.0", "WORKDIR INPATH", optionDescs,
			"Sorts the BAM file in INPATH in a distributed fashion using "+
			"Hadoop. Output parts are placed in WORKDIR.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			verboseOpt, "tell the Hadoop job to be more verbose"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			outputFileOpt, "output a complete BAM file to the file PATH, "+
			               "removing the parts from WORKDIR"));
	}

	@Override protected int run(CmdLineParser parser) {
		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("sort :: OUTDIR not given.");
			return 3;
		}
		if (args.size() == 1) {
			System.err.println("sort :: INPATH not given.");
			return 3;
		}

		final String wrkDir = args.get(0),
		             in     = args.get(1),
		             out    = (String)parser.getOptionValue(outputFileOpt);

		final boolean verbose = parser.getBoolean(verboseOpt);

		final Path   inPath     = new Path(in),
		             wrkDirPath = new Path(wrkDir);
		final String inFile     = inPath.getName();

		final Configuration conf = getConf();

		// Used by SortOutputFormat to fetch the SAM header to output and to name
		// the output files, respectively.
		conf.set(SortOutputFormat.INPUT_PATH_PROP,  in);
		conf.set(SortOutputFormat.OUTPUT_NAME_PROP, inFile);

		final Timer t = new Timer();
		try {
			Utils.setSamplingConf(inPath, conf);

			// As far as I can tell there's no non-deprecated way of getting this
			// info. We can silence this warning but not the import.
			@SuppressWarnings("deprecation")
			final int maxReduceTasks =
				new JobClient(new JobConf(conf)).getClusterStatus()
				.getMaxReduceTasks();

			conf.setInt("mapred.reduce.tasks", Math.max(1, maxReduceTasks*9/10));

			final Job job = new Job(conf);

			job.setJarByClass  (Sort.class);
			job.setMapperClass (Mapper.class);
			job.setReducerClass(SortReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setOutputKeyClass   (NullWritable.class);
			job.setOutputValueClass (SAMRecordWritable.class);

			job.setInputFormatClass (BAMInputFormat.class);
			job.setOutputFormatClass(SortOutputFormat.class);

			FileInputFormat .setInputPaths(job, inPath);
			FileOutputFormat.setOutputPath(job, wrkDirPath);

			job.setPartitionerClass(TotalOrderPartitioner.class);

			System.out.println("sort :: Sampling...");
			t.start();

			InputSampler.<LongWritable,SAMRecordWritable>writePartitionFile(
				job,
				new InputSampler.IntervalSampler<LongWritable,SAMRecordWritable>(
					0.01, 100));

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

		if (out != null) try {
			System.out.println("sort :: Merging output...");
			t.start();

			final Path outPath = new Path(out);

			final FileSystem srcFS = wrkDirPath.getFileSystem(conf);
			final FileSystem dstFS =    outPath.getFileSystem(conf);

			// First, place the BAM header.

			final BAMFileWriter w =
				new BAMFileWriter(dstFS.create(outPath), new File(""));

			w.setSortOrder(SAMFileHeader.SortOrder.coordinate, true);

			final SAMFileReader r =
				new SAMFileReader(inPath.getFileSystem(conf).open(inPath));

			w.setHeader(r.getFileHeader());
			r.close();
			w.close();

			// Then, the BAM contents.

			final OutputStream outs = dstFS.append(outPath);

			final FileStatus[] parts = srcFS.globStatus(new Path(
				wrkDir, conf.get(SortOutputFormat.OUTPUT_NAME_PROP) +
				        "-[0-9][0-9][0-9][0-9][0-9][0-9]*"));

			{int i = 0;
			final Timer t2 = new Timer();
			for (final FileStatus part : parts) {
				t2.start();

				final InputStream ins = srcFS.open(part.getPath());
				IOUtils.copyBytes(ins, outs, conf, false);
				ins.close();

				System.out.printf("sort :: Merged part %d in %d.%03d s.\n",
				                  ++i, t2.stopS(), t2.fms());
			}}
			for (final FileStatus part : parts)
				srcFS.delete(part.getPath(), false);

			// Finally, the BGZF terminator.

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

final class SortOutputFormat extends KeyIgnoringBAMOutputFormat<NullWritable> {
	public static final String  INPUT_PATH_PROP = "hadoopbam.sort.input.path",
	                           OUTPUT_NAME_PROP = "hadoopbam.sort.output.name";

	@Override public RecordWriter<NullWritable,SAMRecordWritable>
		getRecordWriter(TaskAttemptContext context)
		throws IOException
	{
		if (super.header == null) {
			final Configuration conf = context.getConfiguration();
			final Path          path = new Path(conf.get(INPUT_PATH_PROP));
			readSAMHeaderFrom(path, path.getFileSystem(conf));
		}
		return super.getRecordWriter(context);
	}

	@Override public Path getDefaultWorkFile(
			TaskAttemptContext context, String ext)
		throws IOException
	{
		String filename  = context.getConfiguration().get(OUTPUT_NAME_PROP);
		String extension = ext.isEmpty() ? ext : "." + ext;
		int    part      = context.getTaskAttemptID().getTaskID().getId();
		return new Path(super.getDefaultWorkFile(context, ext).getParent(),
			filename + "-" + String.format("%06d", part) + extension);
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job)
		throws FileAlreadyExistsException, IOException
	{}
}
