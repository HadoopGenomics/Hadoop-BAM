// File created: 2010-08-11 10:36:08

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileHeader;

import fi.tkk.ics.hadoop.bam.customsamtools.SAMRecord;

/** A convenience class that you can use as a RecordWriter for BAM files.
 *
 * The write function ignores the key, just outputting the SAMRecord.
 */
public class KeyIgnoringBAMRecordWriter<K> extends BAMRecordWriter<K> {
	public KeyIgnoringBAMRecordWriter(
			Path output, Path input, TaskAttemptContext ctx)
		throws IOException
	{
		super(output, input, ctx);
	}
	public KeyIgnoringBAMRecordWriter(
			Path output, SAMFileHeader header, TaskAttemptContext ctx)
		throws IOException
	{
		super(output, header, ctx);
	}
	public KeyIgnoringBAMRecordWriter(OutputStream output, SAMFileHeader header)
		throws IOException
	{
		super(output, header);
	}

	@Override public void write(K ignored, SAMRecordWritable rec) {
		writeAlignment(rec.get());
	}
}
