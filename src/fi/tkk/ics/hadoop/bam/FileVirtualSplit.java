// File created: 2010-08-09 13:06:32

package fi.tkk.ics.hadoop.bam;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FileVirtualSplit extends InputSplit implements Writable {
	private Path file;
	private long vStart;
	private long vEnd;
	private final String[] locations;

	public FileVirtualSplit() {
		locations = null;
	}

	public FileVirtualSplit(Path f, long vs, long ve, String[] locs) {
		file      = f;
		vStart    = vs;
		vEnd      = ve;
		locations = locs;
	}

	@Override public String[] getLocations() { return locations; }

	@Override public long getLength() {
		// Approximate: we don't know here how many blocks are in between two
		// file offsets, so just use the differences between the file offsets
		// (unless it's zero, in which case the beginning aend are in the same
		// block and we can actually give an exact answer).
		final long vsHi   = vStart & ~0xffff;
		final long veHi   = vEnd   & ~0xffff;
		final long hiDiff = veHi - vsHi;
		return hiDiff == 0 ? ((vEnd & 0xffff) - (vStart & 0xffff)) : hiDiff;
	}

	public Path getPath() { return file; }

	public long getStartVirtualOffset() { return vStart; }
	public long   getEndVirtualOffset() { return vEnd;   }

	@Override public void write(DataOutput out) throws IOException {
		Text.writeString(out, file.toString());
		out.writeLong(vStart);
		out.writeLong(vEnd);
	}
	@Override public void readFields(DataInput in) throws IOException {
		file   = new Path(Text.readString(in));
		vStart = in.readLong();
		vEnd   = in.readLong();
	}
}
