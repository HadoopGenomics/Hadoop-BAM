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

// File created: 2013-06-26 16:10:19

package org.seqdoop.hadoop_bam;

import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;

/** A base {@link RecordWriter} for VCF.
 *
 * <p>Handles the output stream, writing the header if requested, and provides
 * the {@link #writeRecord} function for subclasses.</p>
 */
public abstract class VCFRecordWriter<K>
	extends RecordWriter<K,VariantContextWritable>
{
	private VCFCodec codec = new VCFCodec();
	private VariantContextWriter writer;

	private LazyVCFGenotypesContext.HeaderDataCache vcfHeaderDataCache =
		new LazyVCFGenotypesContext.HeaderDataCache();
	private LazyBCFGenotypesContext.HeaderDataCache bcfHeaderDataCache =
		new LazyBCFGenotypesContext.HeaderDataCache();

	/** A VCFHeader is read from the input Path. */
	public VCFRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		final AsciiLineReader r = new AsciiLineReader(
			input.getFileSystem(ctx.getConfiguration()).open(input));

		final FeatureCodecHeader h = codec.readHeader(new AsciiLineReaderIterator(r));
		if (h == null || !(h.getHeaderValue() instanceof VCFHeader))
			throw new IOException("No VCF header found in "+ input);

		r.close();

		init(output, (VCFHeader) h.getHeaderValue(), writeHeader, ctx);
	}
	public VCFRecordWriter(
			Path output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ctx.getConfiguration()).create(output),
			header, writeHeader, ctx);
	}
	public VCFRecordWriter(
			OutputStream output, VCFHeader header, boolean writeHeader)
		throws IOException
	{
		init(output, header, writeHeader, null);
	}

	public VCFRecordWriter(
			OutputStream output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
			throws IOException
	{
		init(output, header, writeHeader, ctx);
	}

	// Working around not being able to call a constructor other than as the
	// first statement...
	private void init(
			Path output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ctx.getConfiguration()).create(output),
			header, writeHeader, ctx);
	}
	private void init(
			OutputStream output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		final StoppableOutputStream stopOut =
			new StoppableOutputStream(!writeHeader, output);

		writer = createVariantContextWriter(ctx == null ? null : ctx.getConfiguration(),
				stopOut);

		writer.writeHeader(header);
		stopOut.stopped = false;

		setInputHeader(header);
	}

	protected VariantContextWriter createVariantContextWriter(Configuration conf,
			OutputStream out) {
		return new VariantContextWriterBuilder().clearOptions()
				.setOutputStream(out).build();
	}

	@Override public void close(TaskAttemptContext ctx) throws IOException {
		writer.close();
	}

	/** Used for lazy decoding of genotype data. Of course, each input record
	 * may have a different header, but we currently only support one header
	 * here... This is in part due to the fact that it's not clear what the best
	 * solution is. */
	public void setInputHeader(VCFHeader header) {
		vcfHeaderDataCache.setHeader(header);
		bcfHeaderDataCache.setHeader(header);
	}

	protected void writeRecord(VariantContext vc) {
		final GenotypesContext gc = vc.getGenotypes();
		if (gc instanceof LazyParsingGenotypesContext)
			((LazyParsingGenotypesContext)gc).getParser().setHeaderDataCache(
				gc instanceof LazyVCFGenotypesContext ? vcfHeaderDataCache
				                                      : bcfHeaderDataCache);

		writer.add(vc);
	}
}

// We must always call writer.writeHeader() because the writer requires
// the header in writer.add(), and writeHeader() is the only way to give
// the header to the writer. Thus, we use this class to simply throw away
// output until after the header's been written.
//
// This is, of course, a HACK and a slightly dangerous one: if writer
// does any buffering of its own and doesn't flush after writing the
// header, this isn't as easy as this.
final class StoppableOutputStream extends FilterOutputStream {
	public boolean stopped;

	public StoppableOutputStream(boolean startStopped, OutputStream out) {
		super(out);
		stopped = startStopped;
	}

	@Override public void write(int b) throws IOException {
		if (!stopped) super.write(b);
	}
	@Override public void write(byte[] b) throws IOException {
		if (!stopped) super.write(b);
	}
	@Override public void write(byte[] b, int off, int len) throws IOException {
		if (!stopped) super.write(b, off, len);
	}
}
