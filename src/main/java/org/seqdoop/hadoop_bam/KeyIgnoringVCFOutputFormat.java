// Copyright (c) 2013 Aalto University
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

// File created: 2013-06-26 15:19:41

package org.seqdoop.hadoop_bam;

import java.io.IOException;

import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.variant.vcf.VCFHeader;

import org.apache.hadoop.util.ReflectionUtils;
import org.seqdoop.hadoop_bam.util.BGZFCodec;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

/** Writes only the VCF records, not the key.
 *
 * <p>A {@link VCFHeader} must be provided via {@link #setHeader} or {@link
 * #readHeaderFrom} before {@link #getRecordWriter} is called.</p>
 *
 * <p>By default, writes the VCF header to the output file(s). This can be
 * disabled, because in distributed usage one often ends up with (and, for
 * decent performance, wants to end up with) the output split into multiple
 * parts, which are easier to concatenate if the header is not present in each
 * file.</p>
 */
public class KeyIgnoringVCFOutputFormat<K> extends VCFOutputFormat<K> {
	protected VCFHeader header;

	public KeyIgnoringVCFOutputFormat(VCFFormat fmt) { super(fmt); }
	public KeyIgnoringVCFOutputFormat(Configuration conf) {
		super(conf);
		if (format == null)
			throw new IllegalArgumentException(
				"unknown VCF format: OUTPUT_VCF_FORMAT_PROPERTY not set");
	}
	public KeyIgnoringVCFOutputFormat(Configuration conf, Path path) {
		super(conf);
		if (format == null) {
			format = VCFFormat.inferFromFilePath(path);

			if (format == null)
				throw new IllegalArgumentException("unknown VCF format: " + path);
		}
	}

	/** Whether the header will be written, defaults to true. */
	public static final String WRITE_HEADER_PROPERTY =
		"hadoopbam.vcf.write-header";

	public VCFHeader getHeader()                 { return header; }
	public void      setHeader(VCFHeader header) { this.header = header; }

	public void readHeaderFrom(Path path, FileSystem fs) throws IOException {
		SeekableStream i = WrapSeekable.openPath(fs, path);
		readHeaderFrom(i);
		i.close();
	}
	public void readHeaderFrom(SeekableStream in) throws IOException {
		this.header = VCFHeaderReader.readHeaderFrom(in);
	}

	/** <code>setHeader</code> or <code>readHeaderFrom</code> must have been
	 * called first.
	 */
	@Override public RecordWriter<K,VariantContextWritable> getRecordWriter(
			TaskAttemptContext ctx)
		throws IOException
	{
		Configuration conf = ctx.getConfiguration();
		boolean isCompressed = getCompressOutput(ctx);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass =
					getOutputCompressorClass(ctx, BGZFCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(ctx, extension);
		if (!isCompressed) {
			return getRecordWriter(ctx, file);
		} else {
			FileSystem fs = file.getFileSystem(conf);
			return getRecordWriter(ctx, codec.createOutputStream(fs.create(file)));
		}
	}

	// Allows wrappers to provide their own work file.
	public RecordWriter<K,VariantContextWritable> getRecordWriter(
			TaskAttemptContext ctx, Path out)
		throws IOException
	{
		if (this.header == null)
			throw new IOException(
				"Can't create a RecordWriter without the VCF header");

		final boolean wh = ctx.getConfiguration().getBoolean(
			WRITE_HEADER_PROPERTY, true);

		switch (format) {
			case BCF: return new KeyIgnoringBCFRecordWriter<K>(out,header,wh,ctx);
			case VCF: return new KeyIgnoringVCFRecordWriter<K>(out,header,wh,ctx);
			default: assert false; return null;
		}
	}

	public RecordWriter<K,VariantContextWritable> getRecordWriter(
			TaskAttemptContext ctx, OutputStream outputStream)
			throws IOException
	{
		if (this.header == null)
			throw new IOException(
					"Can't create a RecordWriter without the VCF header");

		final boolean wh = ctx.getConfiguration().getBoolean(
				WRITE_HEADER_PROPERTY, true);

		switch (format) {
			case BCF: return new KeyIgnoringBCFRecordWriter<K>(outputStream,header,wh,ctx);
			case VCF: return new KeyIgnoringVCFRecordWriter<K>(outputStream,header,wh,ctx);
			default: assert false; return null;
		}
	}
}
