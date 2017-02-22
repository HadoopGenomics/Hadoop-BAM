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

// File created: 2010-08-09 13:06:32

package org.seqdoop.hadoop_bam;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/** Like a {@link org.apache.hadoop.mapreduce.lib.input.FileSplit}, but uses
 * BGZF virtual offsets to fit with {@link
 * htsjdk.samtools.util.BlockCompressedInputStream}.
 */
public class FileVirtualSplit extends InputSplit implements Writable {
	private Path file;
	private long vStart;
	private long vEnd;
	private final String[] locations;
	private long[] intervalFilePointers;

	private static final String[] NO_LOCATIONS = {};

	public FileVirtualSplit() { locations = NO_LOCATIONS; }

	public FileVirtualSplit(Path f, long vs, long ve, String[] locs) {
		file      = f;
		vStart    = vs;
		vEnd      = ve;
		locations = locs;
	}

	public FileVirtualSplit(Path f, long vs, long ve, String[] locs, long[] intervalFilePointers) {
		file      = f;
		vStart    = vs;
		vEnd      = ve;
		locations = locs;
		this.intervalFilePointers = intervalFilePointers;
	}

	@Override public String[] getLocations() { return locations; }

	/** Inexact due to the nature of virtual offsets.
    *
    * We can't know how many blocks there are in between two file offsets, nor
    * how large those blocks are. So this uses only the difference between the
    * file offsets—unless that difference is zero, in which case the split is
    * wholly contained in one block and thus we can give an exact result.
	 */
	@Override public long getLength() {
		final long vsHi   = vStart & ~0xffff;
		final long veHi   = vEnd   & ~0xffff;
		final long hiDiff = veHi - vsHi;
		return hiDiff == 0 ? ((vEnd & 0xffff) - (vStart & 0xffff)) : hiDiff;
	}

	public Path getPath() { return file; }

	/** Inclusive. */
	public long getStartVirtualOffset() { return vStart; }

	/** Exclusive. */
	public long   getEndVirtualOffset() { return vEnd;   }

	public void setStartVirtualOffset(long vo) { vStart = vo; }
	public void   setEndVirtualOffset(long vo) { vEnd   = vo; }

	/**
	 * @return pairs of virtual file pointers for all intervals that should be used for
	 * filtering the split, or <code>null</code> if there are none. These correspond to
	 * BAMFileSpan chunk start/stop pointers in htsjdk.
	 */
	public long[] getIntervalFilePointers() {
		return intervalFilePointers;
	}

	@Override public void write(DataOutput out) throws IOException {
		Text.writeString(out, file.toString());
		out.writeLong(vStart);
		out.writeLong(vEnd);
		out.writeBoolean(intervalFilePointers != null);
		if (intervalFilePointers != null) {
			out.writeInt(intervalFilePointers.length);
			for (int i = 0; i < intervalFilePointers.length; i++) {
				out.writeLong(intervalFilePointers[i]);
			}
		}
	}
	@Override public void readFields(DataInput in) throws IOException {
		file   = new Path(Text.readString(in));
		vStart = in.readLong();
		vEnd   = in.readLong();
		if (in.readBoolean()) {
			intervalFilePointers = new long[in.readInt()];
			for (int i = 0; i < intervalFilePointers.length; i++) {
				intervalFilePointers[i] = in.readLong();
			}
		}
	}

	@Override
	public String toString() { return file + ":" + vStart + "-" + vEnd; }
}
