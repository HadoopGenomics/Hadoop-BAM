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

// File created: 2010-08-10 13:03:10

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;

import java.nio.charset.Charset;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

/** A base {@link RecordWriter} for BAM records.
 *
 * <p>Handles the output stream, writing the header if requested, and provides
 * the {@link #writeAlignment} function for subclasses.</p>
 */
public abstract class BAMRecordWriter<K>
	extends RecordWriter<K,SAMRecordWritable>
{
	private OutputStream   origOutput;
	private BinaryCodec    binaryCodec;
	private BAMRecordCodec recordCodec;
	private BlockCompressedOutputStream compressedOut;
	private SplittingBAMIndexer splittingBAMIndexer;

	/** A SAMFileHeader is read from the input Path. */
	public BAMRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output,
			SAMHeaderReader.readSAMHeaderFrom(input, ctx.getConfiguration()),
			writeHeader, ctx);
		if (ctx.getConfiguration().getBoolean(BAMOutputFormat.WRITE_SPLITTING_BAI, false)) {
			Path splittingIndex = BAMInputFormat.getIdxPath(output);
			OutputStream splittingIndexOutput =
					output.getFileSystem(ctx.getConfiguration()).create(splittingIndex);
			splittingBAMIndexer = new SplittingBAMIndexer(splittingIndexOutput);
		}
	}
	public BAMRecordWriter(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ctx.getConfiguration()).create(output),
			header, writeHeader, BAMOutputFormat.useIntelDeflater(ctx.getConfiguration()));
		if (ctx.getConfiguration().getBoolean(BAMOutputFormat.WRITE_SPLITTING_BAI, false)) {
			Path splittingIndex = BAMInputFormat.getIdxPath(output);
			OutputStream splittingIndexOutput =
					output.getFileSystem(ctx.getConfiguration()).create(splittingIndex);
			splittingBAMIndexer = new SplittingBAMIndexer(splittingIndexOutput);
		}
	}

	// Working around not being able to call a constructor other than as the
	// first statement...
	private void init(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ctx.getConfiguration()).create(output),
			header, writeHeader, BAMOutputFormat.useIntelDeflater(ctx.getConfiguration()));
	}
	private void init(
			OutputStream output, SAMFileHeader header, boolean writeHeader,
			boolean useIntelDeflater)
		throws IOException
	{
		origOutput = output;

		if (useIntelDeflater) {
                    compressedOut = new BlockCompressedOutputStream(origOutput, (File) null,
					BlockCompressedStreamConstants.DEFAULT_COMPRESSION_LEVEL,
					IntelGKLAccessor.newDeflaterFactory());
		} else {
                    compressedOut = new BlockCompressedOutputStream(origOutput, (File) null);
		}

		binaryCodec = new BinaryCodec(compressedOut);
		recordCodec = new BAMRecordCodec(header);
		recordCodec.setOutputStream(compressedOut);

		if (writeHeader)
			this.writeHeader(header);
	}

	@Override public void close(TaskAttemptContext ctx) throws IOException {
		// Don't close the codec, we don't want BlockCompressedOutputStream's
		// file terminator to be output. But do flush the stream.
		binaryCodec.getOutputStream().flush();

		// Finish indexer with file length
		if (splittingBAMIndexer != null) {
			splittingBAMIndexer.finish(compressedOut.getFilePointer() >> 16);
		}

		// And close the original output.
		origOutput.close();
	}

	protected void writeAlignment(final SAMRecord rec) throws IOException {
		if (splittingBAMIndexer != null) {
			splittingBAMIndexer.processAlignment(compressedOut.getFilePointer());
		}
		recordCodec.encode(rec);
	}

	private void writeHeader(final SAMFileHeader header) {
		binaryCodec.writeBytes("BAM\001".getBytes(Charset.forName("UTF8")));

		final Writer sw = new StringWriter();
		new SAMTextHeaderCodec().encode(sw, header);

		binaryCodec.writeString(sw.toString(), true, false);

		final SAMSequenceDictionary dict = header.getSequenceDictionary();

		binaryCodec.writeInt(dict.size());
		for (final SAMSequenceRecord rec : dict.getSequences()) {
			binaryCodec.writeString(rec.getSequenceName(), true, true);
			binaryCodec.writeInt   (rec.getSequenceLength());
		}
	}
}
