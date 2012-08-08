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

// File created: 2010-08-09 14:34:08

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.util.BlockCompressedInputStream;

import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

/** The key is the bitwise OR of the reference sequence ID in the upper 32 bits
 * and the 0-based leftmost coordinate in the lower.
 */
public class BAMRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private final LongWritable key = new LongWritable();
	private final SAMRecordWritable record = new SAMRecordWritable();

	private BlockCompressedInputStream bci;
	private BAMRecordCodec codec;
	private long fileStart, virtualEnd;

	private static final Random random = new Random();

	public static long getKey(final SAMRecord rec) {
		int refIdx = rec.getReferenceIndex();

		// Pass on the fact that this is an unmapped read.
		if (rec.getReadUnmappedFlag())
			refIdx = -1;

		return getKey(refIdx, rec.getAlignmentStart());
	}

	/** @param alignmentStart 1-based leftmost coordinate. */
	public static long getKey(int refIdx, int alignmentStart) {
		return getKey0(refIdx, alignmentStart-1);
	}

	/** @param alignmentStart0 0-based leftmost coordinate. */
	public static long getKey0(int refIdx, int alignmentStart0) {
		// Put unmapped reads at the end, but don't give them all the exact same
		// key so that they can be distributed to different reducers.
		if (refIdx < 0 || alignmentStart0 < 0) {
			refIdx = Integer.MAX_VALUE;
			alignmentStart0 = random.nextInt();
		}

		return (long)refIdx << 32 | alignmentStart0;
	}

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		final FileVirtualSplit split = (FileVirtualSplit)spl;

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(ctx.getConfiguration());

		final FSDataInputStream in = fs.open(file);
		codec = new BAMRecordCodec(new SAMFileReader(in).getFileHeader());

		in.seek(0);
		bci =
			new BlockCompressedInputStream(
				new WrapSeekable<FSDataInputStream>(
					in, fs.getFileStatus(file).getLen(), file));

		final long virtualStart = split.getStartVirtualOffset();

		fileStart  = virtualStart >>> 16;
		virtualEnd = split.getEndVirtualOffset();

		bci.seek(virtualStart);
		codec.setInputStream(bci);
	}
	@Override public void close() throws IOException { bci.close(); }

	/** Unless the end has been reached, this only takes file position into
	 * account, not the position within the block.
	 */
	@Override public float getProgress() {
		final long virtPos = bci.getFilePointer();
		final long filePos = virtPos >>> 16;
		if (virtPos >= virtualEnd)
			return 1;
		else {
			final long fileEnd = virtualEnd >>> 16;
			// Add 1 to the denominator to make sure it doesn't reach 1 here when
			// filePos == fileEnd.
			return (float)(filePos - fileStart) / (fileEnd - fileStart + 1);
		}
	}
	@Override public LongWritable      getCurrentKey  () { return key; }
	@Override public SAMRecordWritable getCurrentValue() { return record; }

	@Override public boolean nextKeyValue() {
		if (bci.getFilePointer() >= virtualEnd)
			return false;

		final SAMRecord r = codec.decode();
		if (r == null)
			return false;

		key.set(getKey(r));
		record.set(r);
		return true;
	}
}
