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

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.broad.tribble.readers.AsciiLineReader;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.vcf.VCFCodec;
import org.broadinstitute.variant.vcf.VCFContigHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeader;

import fi.tkk.ics.hadoop.bam.util.MurmurHash3;

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
	private final LongWritable          key = new LongWritable();
	private final VariantContextWritable vc = new VariantContextWritable();

	private VCFCodec codec = new VCFCodec();
	private AsciiLineReader reader;

	private long start, length;

	private final Map<String,Integer> contigDict =
		new HashMap<String,Integer>();

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		final FileSplit split = (FileSplit)spl;

		this.start  = split.getStart();
		this.length = split.getLength();

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(ctx.getConfiguration());

		final FSDataInputStream ins = fs.open(file);

		reader = new AsciiLineReader(ins);
		final Object h = codec.readHeader(reader);
		if (!(h instanceof VCFHeader))
			throw new IOException("No VCF header found in "+ file);

		contigDict.clear();
		int i = 0;
		for (final VCFContigHeaderLine contig : ((VCFHeader)h).getContigLines())
			contigDict.put(contig.getID(), i++);

		if (start != 0) {
			ins.seek(start-1);
			reader = new AsciiLineReader(ins);
			reader.readLine();
		}
	}
	@Override public void close() { reader.close(); }

	@Override public float getProgress() {
		return length == 0 ? 1 : (float)(reader.getPosition() - start) / length;
	}

	@Override public LongWritable           getCurrentKey  () { return key; }
	@Override public VariantContextWritable getCurrentValue() { return vc; }

	@Override public boolean nextKeyValue() throws IOException {
		if (reader.getPosition() >= start + length)
			return false;

		final String line = reader.readLine();
		if (line == null)
			return false;

		final VariantContext v = codec.decode(line);

		Integer chromIdx = contigDict.get(v.getChr());
		if (chromIdx == null)
			chromIdx = (int)MurmurHash3.murmurhash3(v.getChr(), 0);

		key.set((long)chromIdx << 32 | (long)(v.getStart() - 1));
		vc.set(v);
		return true;
	}
}
