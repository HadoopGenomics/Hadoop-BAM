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

public class KeyIgnoringBAMOutputFormat<K> extends BAMOutputFormat<K> {
	protected SAMFileHeader header;
	private boolean writeHeader = false;

	public KeyIgnoringBAMOutputFormat() {}

	// Defaults to false: only the BAM records will be written.
	public boolean getWriteHeader()          { return writeHeader; }
	public void    setWriteHeader(boolean b) { writeHeader = b; }

	public void setSAMHeader(SAMFileHeader header) { this.header = header; }

	public void readSAMHeaderFrom(Path path, FileSystem fs) throws IOException {
		this.header = new SAMFileReader(fs.open(path)).getFileHeader();
	}
	public void readSAMHeaderFrom(InputStream in) {
		this.header = new SAMFileReader(in).getFileHeader();
	}

	/** setSAMHeader or readSAMHeaderFrom must have been called first. */
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
