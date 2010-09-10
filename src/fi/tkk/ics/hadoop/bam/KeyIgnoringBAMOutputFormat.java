// File created: 2010-08-11 12:19:23

package fi.tkk.ics.hadoop.bam;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;

/** Writes only the BAM records, not the key.
 *
 * <p>A {@link SAMFileHeader} must be provided via {@link #setSAMHeader} or
 * {@link #readSAMHeaderFrom} before {@link #getRecordWriter} is called.</p>
 *
 * <p>Optionally, the BAM header may be written to the output file(s). This
 * defaults to false, because in distributed usage one often ends up with (and,
 * for decent performance, wants to end up with) multiple files, and one does
 * not want the header in each file.</p>
 */
public class KeyIgnoringBAMOutputFormat<K> extends BAMOutputFormat<K> {
	protected SAMFileHeader header;
	private boolean writeHeader = false;

	public KeyIgnoringBAMOutputFormat() {}

	/** Whether the header will be written or not. */
	public boolean getWriteHeader()          { return writeHeader; }

	/** Set whether the header will be written or not. */
	public void    setWriteHeader(boolean b) { writeHeader = b; }

	public void setSAMHeader(SAMFileHeader header) { this.header = header; }

	public void readSAMHeaderFrom(Path path, FileSystem fs) throws IOException {
		InputStream i = fs.open(path);
		readSAMHeaderFrom(i);
		i.close();
	}
	public void readSAMHeaderFrom(InputStream in) {
		this.header = new SAMFileReader(in).getFileHeader();
	}

	/** <code>setSAMHeader</code> or <code>readSAMHeaderFrom</code> must have
	 * been called first.
	 */
	@Override public RecordWriter<K,SAMRecordWritable> getRecordWriter(
			TaskAttemptContext ctx)
		throws IOException
	{
		if (this.header == null)
			throw new IOException(
				"Can't create a RecordWriter without the SAM header");

		return new KeyIgnoringBAMRecordWriter<K>(
			getDefaultWorkFile(ctx, ""), this.header, this.writeHeader, ctx);
	}
}
