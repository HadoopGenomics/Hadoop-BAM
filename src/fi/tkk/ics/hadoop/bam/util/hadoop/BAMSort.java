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

package fi.tkk.ics.hadoop.bam.util.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fi.tkk.ics.hadoop.bam.custom.hadoop.InputSampler;
import fi.tkk.ics.hadoop.bam.custom.hadoop.TotalOrderPartitioner;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public final class BAMSort extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new BAMSort(), args));
	}

	private final List<Job> jobs = new ArrayList<Job>();

	@Override public int run(String[] args)
		throws ClassNotFoundException, IOException, InterruptedException
	{
		if (args.length < 2) {
			System.err.println(
				"Usage: BAMSort <output directory> file [file...]\n\n"+

				"Sorts the alignments in each input file according to their "+
				"leftmost position.\nThe output is in BAM file format, but does "+
				"not include the BAM header at the\nstart nor the proper BGZF "+
				"terminator block at the end. (This is because, in\ndistributed "+
				"usage, the output is split into multiple parts, and it's easier "+
				"to\nadd the header and terminator afterwards than to remove it "+
				"from all of them but\nthe first and last, respectively.)");
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

		// Used by SortOutputFormat to construct the output filename and to fetch
		// the SAM header to output
		conf.set(SortOutputFormat.INPUT_PATH_PROP, inputFile.toString());

		setSamplingConf(inputFile, conf);

		// As far as I can tell there's no non-deprecated way of getting this
		// info. We can silence this warning but not the import.
		@SuppressWarnings("deprecation")
		int maxReduceTasks =
			new JobClient(new JobConf(conf)).getClusterStatus()
			.getMaxReduceTasks();

		conf.setInt("mapred.reduce.tasks", Math.max(1, maxReduceTasks * 9 / 10));

		Job job = new Job(conf);

		job.setJarByClass  (BAMSort.class);
		job.setMapperClass (Mapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setOutputKeyClass   (NullWritable.class);
		job.setOutputValueClass (SAMRecordWritable.class);

		job.setInputFormatClass (BAMInputFormat.class);
		job.setOutputFormatClass(SortOutputFormat.class);

		FileInputFormat .setInputPaths(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setPartitionerClass(TotalOrderPartitioner.class);

		sample(inputFile, job);

		job.submit();
		jobs.add(job);
	}

	private void setSamplingConf(Path inputFile, Configuration conf)
		throws IOException
	{
		Path inputDir = inputFile.getParent();
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));

		String name = inputFile.getName();

		Path partition = new Path(inputDir, "_partitioning" + name);
		TotalOrderPartitioner.setPartitionFile(conf, partition);

		try {
			URI partitionURI = new URI(
				partition.toString() + "#_partitioning" + name);
			DistributedCache.addCacheFile(partitionURI, conf);
			DistributedCache.createSymlink(conf);
		} catch (URISyntaxException e) { assert false; }
	}

	private void sample(Path inputFile, Job job)
		throws ClassNotFoundException, IOException, InterruptedException
	{
		InputSampler.Sampler<LongWritable,SAMRecordWritable> sampler =
			new InputSampler.IntervalSampler<LongWritable,SAMRecordWritable>(
				0.01, 100);

		InputSampler.<LongWritable,SAMRecordWritable>writePartitionFile(
			job, sampler);
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
	public static final String INPUT_PATH_PROP = "bamsort.input.path";

	@Override public RecordWriter<NullWritable,SAMRecordWritable>
		getRecordWriter(TaskAttemptContext context)
		throws IOException
	{
		if (super.header == null) {
			Configuration c = context.getConfiguration();
			readSAMHeaderFrom(
				new Path(c.get(INPUT_PATH_PROP)), FileSystem.get(c));
		}
		return super.getRecordWriter(context);
	}

	@Override public Path getDefaultWorkFile(
			TaskAttemptContext context, String ext)
		throws IOException
	{
		String path      = context.getConfiguration().get(INPUT_PATH_PROP);
		String filename  = new Path(path).getName();
		String extension = ext.isEmpty() ? ext : "." + ext;
		int    part      = context.getTaskAttemptID().getTaskID().getId();
		return new Path(super.getDefaultWorkFile(context, ext).getParent(),
			filename + "-" + String.format("%06d", part) + extension);
	}

	// Allow the output directory to exist, so that we can make multiple jobs
	// that write into it.
	@Override public void checkOutputSpecs(JobContext job)
		throws FileAlreadyExistsException, IOException
	{}
}
