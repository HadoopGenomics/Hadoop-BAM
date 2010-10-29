// File created: 2010-08-11 10:36:08

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileHeader;

import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;

/** A convenience class that you can use as a RecordWriter for BAM files.
 *
 * <p>The write function ignores the key, just outputting the SAMRecord.</p>
 */
public class KeyIgnoringBAMRecordWriter<K> extends BAMRecordWriter<K> {
	public KeyIgnoringBAMRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		super(output, input, writeHeader, ctx);
	}
	public KeyIgnoringBAMRecordWriter(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		super(output, header, writeHeader, ctx);
	}
	public KeyIgnoringBAMRecordWriter(
			OutputStream output, SAMFileHeader header, boolean writeHeader)
		throws IOException
	{
		super(output, header, writeHeader);
	}

	@Override public void write(K ignored, SAMRecordWritable rec) {
		writeAlignment(rec.get());
	}
}
