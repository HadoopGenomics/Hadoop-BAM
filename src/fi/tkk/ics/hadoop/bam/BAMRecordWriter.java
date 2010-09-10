// File created: 2010-08-10 13:03:10

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.util.BinaryCodec;

import fi.tkk.ics.hadoop.bam.customsamtools.BAMRecordCodec;
import fi.tkk.ics.hadoop.bam.customsamtools.BlockCompressedOutputStream;
import fi.tkk.ics.hadoop.bam.customsamtools.SAMRecord;

/** A base {@link RecordWriter} for BAM records.
 *
 * <p>Handles the output stream, writing the header if requested, and provides
 * the {@link #writeAlignment} function for subclasses.</p>
 */
public abstract class BAMRecordWriter<K>
	extends RecordWriter<K,SAMRecordWritable>
{
	private BinaryCodec    binaryCodec;
	private BAMRecordCodec recordCodec;

	/** A SAMFileHeader is read from the input Path. */
	public BAMRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		final SAMFileReader r =
			new SAMFileReader(FileSystem.get(ctx.getConfiguration()).open(input));
		final SAMFileHeader hdr = r.getFileHeader();
		r.close();
		init(output, hdr, writeHeader, ctx);
	}
	public BAMRecordWriter(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			FileSystem.get(ctx.getConfiguration()).create(output),
			header, writeHeader);
	}
	public BAMRecordWriter(
			OutputStream output, SAMFileHeader header, boolean writeHeader)
		throws IOException
	{
		init(output, header, writeHeader);
	}

	// Working around not being able to call a constructor other than as the
	// first statement...
	private void init(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			FileSystem.get(ctx.getConfiguration()).create(output),
			header, writeHeader);
	}
	private void init(
			OutputStream output, SAMFileHeader header, boolean writeHeader)
		throws IOException
	{
		final OutputStream compressedOut =
			new BlockCompressedOutputStream(output);

		binaryCodec = new BinaryCodec(compressedOut);
		recordCodec = new BAMRecordCodec(header);
		recordCodec.setOutputStream(compressedOut);

		if (writeHeader)
			this.writeHeader(header);
	}

	@Override public void close(TaskAttemptContext ctx) {
		binaryCodec.close();
	}

	protected void writeAlignment(final SAMRecord rec) {
		recordCodec.encode(rec);
	}

	private void writeHeader(final SAMFileHeader header) {
		binaryCodec.writeBytes ("BAM\001".getBytes());
		binaryCodec.writeString(header.getTextHeader(), true, false);

		final SAMSequenceDictionary dict = header.getSequenceDictionary();

		binaryCodec.writeInt(dict.size());
		for (final SAMSequenceRecord rec : dict.getSequences()) {
			binaryCodec.writeString(rec.getSequenceName(), true, true);
			binaryCodec.writeInt   (rec.getSequenceLength());
		}
	}
}
