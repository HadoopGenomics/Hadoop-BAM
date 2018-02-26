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

// File created: 2013-06-26 12:49:12

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.OverlapDetector;
import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.seqdoop.hadoop_bam.util.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The key is the bitwise OR of the chromosome index in the upper 32 bits
 * and the 0-based leftmost coordinate in the lower.
 *
 * The chromosome index is based on the ordering of the contig lines in the VCF
 * header. If a chromosome name that cannot be found in the contig lines is
 * used, that name is instead hashed to form the upper part of the key.
 */
public class VCFRecordReader
	extends RecordReader<LongWritable,VariantContextWritable>
{

	private static final Logger logger = LoggerFactory.getLogger(VCFRecordReader.class);
    
	/** A String property corresponding to a ValidationStringency
	 * value. If set, the given stringency is used when any part of the
	 * Hadoop-BAM library reads VCF.
	 */
	public static final String VALIDATION_STRINGENCY_PROPERTY =
		"hadoopbam.vcfrecordreader.validation-stringency";

	static ValidationStringency getValidationStringency(
		final Configuration conf)
	{
		final String p = conf.get(VALIDATION_STRINGENCY_PROPERTY);
		return p == null ? ValidationStringency.STRICT : ValidationStringency.valueOf(p);
	}

	public static void setValidationStringency(
		final Configuration conf,
		final ValidationStringency stringency)
	{
		conf.set(VALIDATION_STRINGENCY_PROPERTY, stringency.toString());
	}

    
	private final LongWritable          key = new LongWritable();
	private final VariantContextWritable vc = new VariantContextWritable();

	private VCFCodec codec = new VCFCodec();
	private LineRecordReader lineRecordReader = new LineRecordReader();

	private VCFHeader header;

	private final Map<String,Integer> contigDict =
		new HashMap<String,Integer>();

	private List<Interval> intervals;
	private OverlapDetector<Interval> overlapDetector;

	private ValidationStringency stringency;
    
	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		final FileSplit split = (FileSplit)spl;

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(ctx.getConfiguration());

		try (final FSDataInputStream ins = fs.open(file)) {

		    CompressionCodec compressionCodec =
			new CompressionCodecFactory(ctx.getConfiguration()).getCodec(file);
		    AsciiLineReader reader;
		    if (compressionCodec == null) {
			reader = new AsciiLineReader(ins);
		    } else {
			Decompressor decompressor = CodecPool.getDecompressor(compressionCodec);
			CompressionInputStream in = compressionCodec.createInputStream(ins,
										       decompressor);
			reader = new AsciiLineReader(in);
		    }
		    
		    AsciiLineReaderIterator it = new AsciiLineReaderIterator(reader);
		    
		    final FeatureCodecHeader h = codec.readHeader(it);
		    if (h == null || !(h.getHeaderValue() instanceof VCFHeader))
			throw new IOException("No VCF header found in "+ file);
		    
		    header = (VCFHeader) h.getHeaderValue();
		    
		    contigDict.clear();
		    int i = 0;
		    for (final VCFContigHeaderLine contig : header.getContigLines()) {
			contigDict.put(contig.getID(), i++);
		    }
		}
		    
		lineRecordReader.initialize(spl, ctx);

		intervals = VCFInputFormat.getIntervals(ctx.getConfiguration());
		if (intervals != null) {
			overlapDetector = OverlapDetector.create(intervals);
		}

                stringency = VCFRecordReader.getValidationStringency(ctx.getConfiguration());
	}
	@Override public void close() throws IOException { lineRecordReader.close(); }

	@Override public float getProgress() throws IOException {
		return lineRecordReader.getProgress();
	}

	@Override public LongWritable           getCurrentKey  () { return key; }
	@Override public VariantContextWritable getCurrentValue() { return vc; }

	@Override public boolean nextKeyValue() throws IOException {
		while (true) {
			String line;
			while (true) {
				if (!lineRecordReader.nextKeyValue()) {
					return false;
				}
				line = lineRecordReader.getCurrentValue().toString();
				if (!line.startsWith("#")) {
					break;
				}
			}

                        final VariantContext v;
                        try {
				v = codec.decode(line);
			} catch (TribbleException e) {
				if (stringency == ValidationStringency.STRICT) {
					if (logger.isErrorEnabled()) {
						logger.error("Parsing line {} failed with {}.", line, e);
					}
					throw e;
				} else {
					if (stringency == ValidationStringency.LENIENT &&
                                            logger.isWarnEnabled()) {
						logger.warn("Parsing line {} failed with {}. Skipping...",
                                                            line, e);
					}
					continue;
				}
			}

			if (!overlaps(v)) {
				continue;
			}

			Integer chromIdx = contigDict.get(v.getContig());
			if (chromIdx == null)
				chromIdx = (int) MurmurHash3.murmurhash3(v.getContig(), 0);

			key.set((long) chromIdx << 32 | (long) (v.getStart() - 1));
			vc.set(v, header);

			return true;
		}
	}

	private boolean overlaps(VariantContext v) {
		if (intervals == null) {
			return true;
		}
		final Interval interval = new Interval(v.getContig(), v.getStart(), v.getEnd());
		return overlapDetector.overlapsAny(interval);
	}
}
