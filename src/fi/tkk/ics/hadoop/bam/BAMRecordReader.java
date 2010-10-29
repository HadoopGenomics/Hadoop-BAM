// File created: 2010-08-09 14:34:08

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.util.BlockCompressedInputStream;

import fi.tkk.ics.hadoop.bam.custom.samtools.BAMRecordCodec;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
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
		key.set((long)r.getReferenceIndex() << 32
		            | r.getAlignmentStart() - 1);
		record.set(r);
		return true;
	}
}
