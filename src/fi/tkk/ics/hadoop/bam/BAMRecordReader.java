// File created: 2010-08-09 14:34:08

package fi.tkk.ics.hadoop.bam;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.SeekableStream;

import fi.tkk.ics.hadoop.bam.customsamtools.BAMRecordCodec;
import fi.tkk.ics.hadoop.bam.customsamtools.SAMRecord;

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

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		final FileVirtualSplit split = (FileVirtualSplit)spl;

		final Path file = split.getPath();
		final FileSystem fs = FileSystem.get(ctx.getConfiguration());

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
		key.set((long)r.getReferenceIndex() << 32
		            | r.getAlignmentStart() - 1);
		record.set(r);
		return true;
	}
}

// Hadoop and the SAM tools each have their own "seekable stream" abstraction.
// This class wraps Hadoop's so that we can give such a one to
// BlockCompressedInputStream and retain seekability.
class WrapSeekable<S extends InputStream & Seekable> extends SeekableStream {
	private final S    stm;
	private final long len;
	private final Path path;

	public WrapSeekable(final S s, long length, Path p) {
		stm  = s;
		len  = length;
		path = p;
	}

	@Override public String getSource() { return path.toString(); }
	@Override public long   length   () { return len; }

	@Override public void    close() throws IOException { stm.close(); }
	@Override public boolean eof  () throws IOException {
		return stm.getPos() == length();
	}
	@Override public void seek(long pos) throws IOException {
		stm.seek(pos);
	}
	@Override public int read() throws IOException {
		return stm.read();
	}
	@Override public int read(byte[] buf, int offset, int len)
		throws IOException
	{
		return stm.read(buf, offset, len);
	}
}
