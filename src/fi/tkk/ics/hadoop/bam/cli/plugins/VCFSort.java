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

// File created: 2013-06-28 17:17:07

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.util.BlockCompressedStreamConstants;
import org.broadinstitute.variant.variantcontext.writer.Options;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriter;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory;
import org.broadinstitute.variant.vcf.VCFHeader;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.KeyIgnoringVCFOutputFormat;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import fi.tkk.ics.hadoop.bam.VCFFormat;
import fi.tkk.ics.hadoop.bam.VCFInputFormat;
import fi.tkk.ics.hadoop.bam.VCFOutputFormat;
import fi.tkk.ics.hadoop.bam.cli.CLIMRPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.Timer;
import fi.tkk.ics.hadoop.bam.util.VCFHeaderReader;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

import hbparquet.hadoop.util.ContextUtil;

public final class VCFSort extends CLIMRPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		formatOpt      = new  StringOption('F', "format=FMT"),
		noTrustExtsOpt = new BooleanOption("no-trust-exts");

	public VCFSort() {
		super("vcf-sort", "VCF and BCF sorting", "1.0",
			"WORKDIR INPATH", optionDescs,
			"Sorts the VCF or BCF file given as INPATH in a distributed fashion "+
			"using Hadoop MapReduce. Output parts are placed in WORKDIR in, by "+
			"default, headerless and unterminated BCF format.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			noTrustExtsOpt, "detect SAM/BAM files only by contents, "+
			                "never by file extension"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			formatOpt, "select the output format based on FMT: VCF or BCF"));

		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			outputPathOpt, "output a complete VCF/BCF file to the file PATH, "+
			               "removing the parts from WORKDIR; VCF/BCF is chosen "+
			               "by file extension, if appropriate (but -F takes "+
			               "precedence)"));
	}

	@Override protected int run(CmdLineParser parser) {
		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("vcf-sort :: WORKDIR not given.");
			return 3;
		}
		if (args.size() == 1) {
			System.err.println("vcf-sort :: INPATH not given.");
			return 3;
		}
		if (!cacheAndSetProperties(parser))
			return 3;

		Path wrkDir = new Path(args.get(0));
		final Path inPath = new Path(args.get(1));

		final Configuration conf = getConf();

		VCFFormat vcfFormat = null;

		final String f = (String)parser.getOptionValue(formatOpt);
		if (f != null) {
			try { vcfFormat = VCFFormat.valueOf(f.toUpperCase(Locale.ENGLISH)); }
			catch (IllegalArgumentException e) {
				System.err.printf("%s :: invalid format '%s'\n",
				                  getCommandName(), f);
				return 3;
			}
		}
		if (vcfFormat == null)
			vcfFormat = outPath == null ? VCFFormat.BCF
			                            : VCFFormat.inferFromFilePath(outPath);

		conf.setBoolean(VCFInputFormat.TRUST_EXTS_PROPERTY,
		                !parser.getBoolean(noTrustExtsOpt));

		conf.setBoolean(KeyIgnoringVCFOutputFormat.WRITE_HEADER_PROPERTY,
		                outPath == null);

		conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY,
		         vcfFormat.toString());

		// Used by Utils.getMergeableWorkFile() to name the output files.
		final String intermediateOutName =
			(outPath == null ? inPath : outPath).getName();
		conf.set(Utils.WORK_FILENAME_PROPERTY, intermediateOutName);

		conf.set(SortOutputFormat.INPUT_PATH_PROP, inPath.toString());

		final Timer t = new Timer();
		try {
			// Required for path ".", for example.
			wrkDir = wrkDir.getFileSystem(conf).makeQualified(wrkDir);

			Utils.configureSampling(wrkDir, intermediateOutName, conf);

			final Job job = new Job(conf);

			job.setJarByClass  (VCFSort.class);
			job.setMapperClass (Mapper.class);
			job.setReducerClass(VCFSortReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setOutputKeyClass   (NullWritable.class);
			job.setOutputValueClass (VariantContextWritable.class);

			job.setInputFormatClass (VCFInputFormat.class);
			job.setOutputFormatClass(SortOutputFormat.class);

			FileInputFormat.addInputPath  (job, inPath);
			FileOutputFormat.setOutputPath(job, wrkDir);

			job.setPartitionerClass(TotalOrderPartitioner.class);

			System.out.println("vcf-sort :: Sampling...");
			t.start();

			InputSampler.<LongWritable,VariantContextWritable>writePartitionFile(
				job,
				new InputSampler.RandomSampler<LongWritable,VariantContextWritable>
					(0.01, 10000, Math.max(100, reduceTasks)));

			System.out.printf("vcf-sort :: Sampling complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

			job.submit();

			System.out.println("vcf-sort :: Waiting for job completion...");
			t.start();

			if (!job.waitForCompletion(verbose)) {
				System.err.println("vcf-sort :: Job failed.");
				return 4;
			}

			System.out.printf("vcf-sort :: Job complete in %d.%03d s.\n",
			                  t.stopS(), t.fms());

		} catch (IOException e) {
			System.err.printf("vcf-sort :: Hadoop error: %s\n", e);
			return 4;
		} catch (ClassNotFoundException e) { throw new RuntimeException(e); }
		  catch   (InterruptedException e) { throw new RuntimeException(e); }

		if (outPath != null) try {
			System.out.println("vcf-sort :: Merging output...");
			t.start();

			final OutputStream outs = outPath.getFileSystem(conf).create(outPath);

			// First, place the VCF or BCF header.

			final WrapSeekable ins = WrapSeekable.openPath(conf, inPath);
			final VCFHeader header = VCFHeaderReader.readHeaderFrom(ins);
			ins.close();

			final VariantContextWriter writer;

			switch (vcfFormat) {
				case VCF:
					writer = VariantContextWriterFactory.create(
						new FilterOutputStream(outs) {
							@Override public void close() throws IOException {
								this.out.flush();
							}
						}, null, VariantContextWriterFactory.NO_OPTIONS);
					break;

				case BCF:
					writer = VariantContextWriterFactory.create(
						new FilterOutputStream(
							new BlockCompressedOutputStream(outs, null))
						{
							@Override public void close() throws IOException {
								this.out.flush();
							}
						}, null, EnumSet.of(Options.FORCE_BCF));
					break;

				default: assert false; writer = null; break;
			}

			writer.writeHeader(header);
			writer.close();

			// Then, the actual VCF or BCF contents.
			Utils.mergeInto(outs, wrkDir, "", "", conf, "vcf-sort");

			// And if BCF, the BGZF terminator.
			if (vcfFormat == VCFFormat.BCF)
				outs.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);

			outs.close();

			System.out.printf("vcf-sort :: Merging complete in %d.%03d s.\n",
									t.stopS(), t.fms());

		} catch (IOException e) {
			System.err.printf("vcf-sort :: Output merging failed: %s\n", e);
			return 5;
		}
		return 0;
	}
}

final class VCFSortReducer
	extends Reducer<LongWritable,VariantContextWritable,
	                NullWritable,VariantContextWritable>
{
	@Override protected void reduce(
			LongWritable ignored, Iterable<VariantContextWritable> records,
			Reducer<LongWritable,VariantContextWritable,
			        NullWritable,VariantContextWritable>.Context
				ctx)
		throws IOException, InterruptedException
	{
		for (VariantContextWritable rec : records)
			ctx.write(NullWritable.get(), rec);
	}
}

final class SortOutputFormat<K>
	extends FileOutputFormat<K, VariantContextWritable>
{
	public static final String INPUT_PATH_PROP = "hadoopbam.vcfsort.inpath";

	private KeyIgnoringVCFOutputFormat<K> baseOF;

	private void initBaseOF(Configuration conf) {
		if (baseOF == null)
			baseOF = new KeyIgnoringVCFOutputFormat<K>(conf);
	}

	@Override public RecordWriter<K,VariantContextWritable> getRecordWriter(
			TaskAttemptContext context)
		throws IOException
	{
		final Configuration conf = ContextUtil.getConfiguration(context);
		initBaseOF(conf);

		if (baseOF.getHeader() == null) {
			final Path p = new Path(conf.get(INPUT_PATH_PROP));
			baseOF.readHeaderFrom(p, p.getFileSystem(conf));
		}

		return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
	}

	@Override public Path getDefaultWorkFile(TaskAttemptContext ctx, String ext)
		throws IOException
	{
		initBaseOF(ContextUtil.getConfiguration(ctx));
		return Utils.getMergeableWorkFile(
			baseOF.getDefaultWorkFile(ctx, ext).getParent(), "", "", ctx, ext);
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job) {}
}
