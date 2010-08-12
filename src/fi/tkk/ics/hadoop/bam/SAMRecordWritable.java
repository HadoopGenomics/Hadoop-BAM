// File created: 2010-08-12 09:57:45

package fi.tkk.ics.hadoop.bam;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.Writable;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.util.BinaryCodec;

import fi.tkk.ics.hadoop.bam.customsamtools.BAMRecordCodec;
import fi.tkk.ics.hadoop.bam.customsamtools.LazyBAMRecordCodec;
import fi.tkk.ics.hadoop.bam.customsamtools.SAMRecord;

public class SAMRecordWritable implements Writable {
	private SAMRecord record;

	public SAMRecord get()            { return record; }
	public void      set(SAMRecord r) { record = r; }

	@Override public void write(DataOutput out) throws IOException {
		// Passing null is somewhat risky but we don't have much choice: we can't
		// get a SAMFileHeader here from anywhere.
		//
		// In theory, it shouldn't matter, since the representation of an
		// alignment in BAM doesn't depend on the header data at all. Only its
		// interpretation does, and a simple read/write codec shouldn't really
		// have anything to say about that.
		//
		// But in practice, it already does matter for decode(), which is why
		// LazyBAMRecordCodec exists. If this does blow up one day, we need to do
		// a similar copy-paste job for BAMRecordCodec.encode() and/or the
		// classes it depends on. (Unless you're in less of a hurry and want to
		// file bug reports.)
		final BAMRecordCodec codec = new BAMRecordCodec(null);
		codec.setOutputStream(new DataOutputWrapper(out));
		codec.encode(record);
	}
	@Override public void readFields(DataInput in) throws IOException {
		final LazyBAMRecordCodec codec = new LazyBAMRecordCodec();
		codec.setInputStream(new DataInputWrapper(in));
		record = codec.decode();
	}
}

class DataOutputWrapper extends OutputStream {
	private final DataOutput out;

	public DataOutputWrapper(DataOutput o) { out = o; }

	@Override public void write(int b) throws IOException {
		out.writeByte(b);
	}
	@Override public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
	}
}

class DataInputWrapper extends InputStream {
	private final DataInput in;

	public DataInputWrapper(DataInput i) { in = i; }

	@Override public long skip(long n) throws IOException {
		for (; n > Integer.MAX_VALUE; n -= Integer.MAX_VALUE) {
			final int skipped = in.skipBytes(Integer.MAX_VALUE);
			if (skipped < Integer.MAX_VALUE)
				return skipped;
		}
		return in.skipBytes((int)n);
	}
	@Override public int read(byte[] b, int off, int len) throws IOException {
		in.readFully(b, off, len);
		return len;
	}
	@Override public int read() throws IOException {
		return in.readByte();
	}
}
