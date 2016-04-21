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

package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileHeader.SortOrder;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.CoordMath;
import htsjdk.samtools.util.Locatable;

import org.seqdoop.hadoop_bam.util.MurmurHash3;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

/** The key is the bitwise OR of the reference sequence ID in the upper 32 bits
 * and the 0-based leftmost coordinate in the lower.
 */
public class BAMRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private final LongWritable key = new LongWritable();
	private final SAMRecordWritable record = new SAMRecordWritable();

	private ValidationStringency stringency;

	private BlockCompressedInputStream bci;
	private BAMRecordCodec codec;
	private long fileStart, virtualStart, virtualEnd;
	private boolean isInitialized = false;
	private boolean keepReadPairsTogether;
	private boolean readPair;
	private boolean lastOfPair;
	private List<Locatable> intervals;

	/** Note: this is the only getKey function that handles unmapped reads
	 * specially!
	 */
	public static long getKey(final SAMRecord rec) {
		final int refIdx = rec.getReferenceIndex();
		final int start  = rec.getAlignmentStart();

		if (!(rec.getReadUnmappedFlag() || refIdx < 0 || start < 0))
			return getKey(refIdx, start);

		// Put unmapped reads at the end, but don't give them all the exact same
		// key so that they can be distributed to different reducers.
		//
		// A random number would probably be best, but to ensure that the same
		// record always gets the same key we use a fast hash instead.
		//
		// We avoid using hashCode(), because it's not guaranteed to have the
		// same value across different processes.

		int hash = 0;
		byte[] var;
		if ((var = rec.getVariableBinaryRepresentation()) != null) {
			// Undecoded BAM record: just hash its raw data.
			hash = (int)MurmurHash3.murmurhash3(var, hash);
		} else {
			// Decoded BAM record or any SAM record: hash a few representative
			// fields together.
			hash = (int)MurmurHash3.murmurhash3(rec.getReadName(), hash);
			hash = (int)MurmurHash3.murmurhash3(rec.getReadBases(), hash);
			hash = (int)MurmurHash3.murmurhash3(rec.getBaseQualities(), hash);
			hash = (int)MurmurHash3.murmurhash3(rec.getCigarString(), hash);
		}
		return getKey0(Integer.MAX_VALUE, hash);
	}

	/** @param alignmentStart 1-based leftmost coordinate. */
	public static long getKey(int refIdx, int alignmentStart) {
		return getKey0(refIdx, alignmentStart-1);
	}

	/** @param alignmentStart0 0-based leftmost coordinate. */
	public static long getKey0(int refIdx, int alignmentStart0) {
		return (long)refIdx << 32 | alignmentStart0;
	}

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		// This method should only be called once (see Hadoop API). However,
		// there seems to be disagreement between implementations that call
		// initialize() and Hadoop-BAM's own code that relies on
		// {@link BAMInputFormat} to call initialize() when the reader is
		// created. Therefore we add this check for the time being. 
		if(isInitialized)
			close();
		isInitialized = true;

		final Configuration conf = ctx.getConfiguration();

		final FileVirtualSplit split = (FileVirtualSplit)spl;
		final Path             file  = split.getPath();
		final FileSystem       fs    = file.getFileSystem(conf);

		this.stringency = SAMHeaderReader.getValidationStringency(conf);

		final FSDataInputStream in = fs.open(file);

		final SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(in, conf);
		codec = new BAMRecordCodec(header);

		in.seek(0);
		bci =
			new BlockCompressedInputStream(
				new WrapSeekable<FSDataInputStream>(
					in, fs.getFileStatus(file).getLen(), file));

		virtualStart = split.getStartVirtualOffset();

		fileStart  = virtualStart >>> 16;
		virtualEnd = split.getEndVirtualOffset();

		bci.seek(virtualStart);
		codec.setInputStream(bci);
		
		if(BAMInputFormat.DEBUG_BAM_SPLITTER) {
			final long recordStart = virtualStart & 0xffff;
                	System.err.println("XXX inizialized BAMRecordReader byte offset: " +
				fileStart + " record offset: " + recordStart);
		}

		keepReadPairsTogether = SortOrder.queryname.equals(header.getSortOrder()) &&
			conf.getBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY, false);
		readPair = false;
		lastOfPair = false;
		intervals = BAMInputFormat.getIntervals(conf);
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
		long virtPos;
		while ((virtPos = bci.getFilePointer()) < virtualEnd || (keepReadPairsTogether && readPair && !lastOfPair)) {

			final SAMRecord r = codec.decode();
			if (r == null)
				return false;

			// Since we're reading from a BAMRecordCodec directly we have to set the
			// validation stringency ourselves.
			if (this.stringency != null)
				r.setValidationStringency(this.stringency);

			readPair = r.getReadPairedFlag();
			if (readPair) {
				boolean first = r.getFirstOfPairFlag(), second = r.getSecondOfPairFlag();
				// According to the SAM spec (section 1.4) it is possible for pairs to have
				// multiple segments (i.e. more than two), in which case both `first` and
				// `second` will be true.
				boolean firstOfPair = first && !second;
				lastOfPair = !first && second;
				// ignore any template that is not first in a pair right at the start of a split
				// since it will have been returned in the previous split
				if (virtPos == virtualStart && keepReadPairsTogether && !firstOfPair) {
					continue;
				}
			}

			if (!overlaps(r, intervals)) {
				continue;
			}

			key.set(getKey(r));
			record.set(r);
			return true;
		}
		return false;
	}

	private static boolean overlaps(SAMRecord r, List<Locatable> intervals) {
		if (intervals == null ||
				(r.getReadUnmappedFlag() && r.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START)) {
			return true;
		}
		for (Locatable interval : intervals) {
			if (r.getReadUnmappedFlag()) {
				if (interval.getStart() <= r.getStart() && interval.getEnd() >= r.getStart()) {
					// This follows the behavior of htsjdk's SamReader which states that
					// "an unmapped read will be returned by this call if it has a coordinate for
					// the purpose of sorting that is in the query region".
					return true;
				}
			} else if (r.getContig().equals(interval.getContig()) &&
					CoordMath.overlaps(r.getStart(), r.getEnd(),
						interval.getStart(), interval.getEnd())) {
				return true;
			}
		}
		return false;
	}
}
