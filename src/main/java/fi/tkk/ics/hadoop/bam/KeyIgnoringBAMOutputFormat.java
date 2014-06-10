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

// File created: 2010-08-11 12:19:23

package org.seqdoop.hadoopbam;

import java.io.IOException;
import java.io.InputStream;

import net.sf.samtools.SAMFileHeader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.seqdoop.hadoopbam.util.SAMHeaderReader;

/** Writes only the BAM records, not the key.
 *
 * <p>A {@link SAMFileHeader} must be provided via {@link #setSAMHeader} or
 * {@link #readSAMHeaderFrom} before {@link #getRecordWriter} is called.</p>
 *
 * <p>By default, writes the SAM header to the output file(s). This
 * can be disabled, because in distributed usage one often ends up with (and,
 * for decent performance, wants to end up with) the output split into multiple
 * parts, which are easier to concatenate if the header is not present in each
 * file.</p>
 */
public class KeyIgnoringBAMOutputFormat<K> extends BAMOutputFormat<K> {
	protected SAMFileHeader header;
	private boolean writeHeader = true;

	public KeyIgnoringBAMOutputFormat() {}

	/** Whether the header will be written or not. */
	public boolean getWriteHeader()          { return writeHeader; }

	/** Set whether the header will be written or not. */
	public void    setWriteHeader(boolean b) { writeHeader = b; }

	public SAMFileHeader getSAMHeader() { return header; }
	public void setSAMHeader(SAMFileHeader header) { this.header = header; }

	public void readSAMHeaderFrom(Path path, Configuration conf)
		throws IOException
	{
		this.header = SAMHeaderReader.readSAMHeaderFrom(path, conf);
	}
	public void readSAMHeaderFrom(InputStream in, Configuration conf) {
		this.header = SAMHeaderReader.readSAMHeaderFrom(in, conf);
	}

	/** <code>setSAMHeader</code> or <code>readSAMHeaderFrom</code> must have
	 * been called first.
	 */
	@Override public RecordWriter<K,SAMRecordWritable> getRecordWriter(
			TaskAttemptContext ctx)
		throws IOException
	{
		return getRecordWriter(ctx, getDefaultWorkFile(ctx, ""));
	}

	// Allows wrappers to provide their own work file.
	public RecordWriter<K,SAMRecordWritable> getRecordWriter(
			TaskAttemptContext ctx, Path out)
		throws IOException
	{
		if (this.header == null)
			throw new IOException(
				"Can't create a RecordWriter without the SAM header");

		return new KeyIgnoringBAMRecordWriter<K>(out, header, writeHeader, ctx);
	}
}
