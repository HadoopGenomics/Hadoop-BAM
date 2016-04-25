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

// File created: 2013-06-28 16:12:58

package org.seqdoop.hadoop_bam;

import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.readers.PositionalBufferedStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFHeader;

import org.seqdoop.hadoop_bam.util.MurmurHash3;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

/** See {@link VCFRecordReader} for the meaning of the key. */
public class BCFRecordReader
	extends RecordReader<LongWritable,VariantContextWritable>
{
	private final LongWritable          key = new LongWritable();
	private final VariantContextWritable vc = new VariantContextWritable();

	private BCF2Codec codec = new BCF2Codec();
	private PositionalBufferedStream in;

	private final Map<String,Integer> contigDict =
		new HashMap<String,Integer>();

	private boolean isBGZF;
	private BlockCompressedInputStream bci;

	// If isBGZF, length refers only to the distance of the last BGZF block from
	// the first.
	private long fileStart, length;

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		isBGZF = spl instanceof FileVirtualSplit;
		if (isBGZF) {
			final FileVirtualSplit split = (FileVirtualSplit)spl;

			final Path file = split.getPath();
			final FileSystem fs = file.getFileSystem(ctx.getConfiguration());

			final FSDataInputStream inFile = fs.open(file);

			bci = new BlockCompressedInputStream(inFile);
			in = new PositionalBufferedStream(bci);
			initContigDict();

			inFile.seek(0);
			bci =
				new BlockCompressedInputStream(
					new WrapSeekable<FSDataInputStream>(
						inFile, fs.getFileStatus(file).getLen(), file));

			final long virtualStart = split.getStartVirtualOffset(),
			           virtualEnd   = split.getEndVirtualOffset();

			this.fileStart = virtualStart >>> 16;
			this.length    = (virtualEnd  >>> 16) - fileStart;

			bci.seek(virtualStart);

			// Since PositionalBufferedStream does its own buffering, we have to
			// prevent it from going too far by using a BGZFLimitingStream. It
			// also allows nextKeyValue() to simply check for EOF instead of
			// looking at virtualEnd.
			in = new PositionalBufferedStream(
				new BGZFLimitingStream(bci, virtualEnd));
		} else {
			final FileSplit split = (FileSplit)spl;

			this.fileStart = split.getStart();
			this.length    = split.getLength();

			final Path file = split.getPath();

			in = new PositionalBufferedStream(
				file.getFileSystem(ctx.getConfiguration()).open(file));

			initContigDict();

			IOUtils.skipFully(in, fileStart - in.getPosition());
		}
	}
	@Override public void close() throws IOException { in.close(); }

	private void initContigDict() {
		final VCFHeader header =
			(VCFHeader)codec.readHeader(in).getHeaderValue();

		contigDict.clear();
		int i = 0;
		for (final VCFContigHeaderLine contig : header.getContigLines())
			contigDict.put(contig.getID(), i++);
	}

	/** For compressed BCF, unless the end has been reached, this is quite
	 * inaccurate.
	 */
	@Override public float getProgress() {
		if (length == 0)
			return 1;

		if (!isBGZF)
			return (float)(in.getPosition() - fileStart) / length;

		try {
			if (in.peek() == -1)
				return 1;
		} catch (IOException e) {
			return 1;
		}

		// Add 1 to the denominator to make sure that we never report 1 here.
		return (float)((bci.getFilePointer() >>> 16) - fileStart) / (length + 1);
	}
	@Override public LongWritable           getCurrentKey  () { return key; }
	@Override public VariantContextWritable getCurrentValue() { return vc; }

	@Override public boolean nextKeyValue() throws IOException {
		if (in.peek() == -1)
			return false;

		if (!isBGZF && in.getPosition() >= fileStart + length)
			return false;

		final VariantContext v = codec.decode(in);

		Integer chromIdx = contigDict.get(v.getContig());
		if (chromIdx == null)
			chromIdx = (int)MurmurHash3.murmurhash3(v.getContig(), 0);

		key.set((long)chromIdx << 32 | (long)(v.getStart() - 1));
		vc.set(v);
		return true;
	}
}

class BGZFLimitingStream extends InputStream {
	private final BlockCompressedInputStream bgzf;
	private final long virtEnd;

	public BGZFLimitingStream(
			BlockCompressedInputStream stream, long virtualEnd)
	{
		bgzf = stream;
		virtEnd = virtualEnd;
	}

	@Override public void close() throws IOException { bgzf.close(); }

	private byte[] readBuf = new byte[1];
	@Override public int read() throws IOException {
		switch (read(readBuf)) {
			case  1: return readBuf[0];
			case -1: return -1;
			default: assert false; return -1;
		}
	}

	@Override public int read(byte[] buf, int off, int len) throws IOException {

		int totalRead = 0;
		long virt;

		final int lastLen = (int)virtEnd & 0xffff;

		while ((virt = bgzf.getFilePointer()) >>> 16 != virtEnd >>> 16) {
			// We're not in the last BGZF block yet. Unfortunately
			// BlockCompressedInputStream doesn't expose the length of the current
			// block, so we can't simply (possibly repeatedly) read the current
			// block to the end. Instead, we read at most virtEnd & 0xffff at a
			// time, which ensures that we can't overshoot virtEnd even if the
			// next block starts immediately.
			final int r = bgzf.read(buf, off, Math.min(len, lastLen));
			if (r == -1)
				return totalRead == 0 ? -1 : totalRead;

			totalRead += r;
			len -= r;
			if (len == 0)
				return totalRead;
			off += r;
		}

		// We're in the last BGZF block: read only up to lastLen.
		len = Math.min(len, ((int)virt & 0xffff) - lastLen);
		while (len > 0) {
			final int r = bgzf.read(buf, off, len);
			if (r == -1)
				return totalRead == 0 ? -1 : totalRead;

			totalRead += r;
			len -= r;
			off += r;
		}
		return totalRead == 0 ? -1 : totalRead;
	}
}
