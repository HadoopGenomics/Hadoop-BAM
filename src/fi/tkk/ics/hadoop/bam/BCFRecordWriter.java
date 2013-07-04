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

// File created: 2013-06-28 15:58:08

package fi.tkk.ics.hadoop.bam;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.util.BlockCompressedOutputStream;
import org.broadinstitute.variant.variantcontext.GenotypesContext;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.variantcontext.writer.Options;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriter;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory;
import org.broadinstitute.variant.vcf.VCFHeader;

import fi.tkk.ics.hadoop.bam.util.VCFHeaderReader;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

/** A base {@link RecordWriter} for compressed BCF.
 *
 * <p>Handles the output stream, writing the header if requested, and provides
 * the {@link #writeRecord} function for subclasses.</p>
 */
public abstract class BCFRecordWriter<K>
	extends RecordWriter<K,VariantContextWritable>
{
	private VariantContextWriter writer;
	private VCFHeader header;

	/** A VCF header is read from the input Path, which should refer to a VCF or
	 * BCF file.
	 */
	public BCFRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		final WrapSeekable in =
			WrapSeekable.openPath(ctx.getConfiguration(), input);
		final VCFHeader header = VCFHeaderReader.readHeaderFrom(in);
		in.close();

		init(output, header, writeHeader, ctx);
	}
	public BCFRecordWriter(
			Path output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ctx.getConfiguration()).create(output),
			header, writeHeader);
	}
	public BCFRecordWriter(
			OutputStream output, VCFHeader header, boolean writeHeader)
		throws IOException
	{
		init(output, header, writeHeader);
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
			header, writeHeader);
	}
	private void init(
			OutputStream output, VCFHeader header, final boolean writeHeader)
		throws IOException
	{
		final BCFStoppableOutputStream stopOut =
			new BCFStoppableOutputStream(!writeHeader, output);

		writer = VariantContextWriterFactory.create(
			stopOut, null, EnumSet.of(Options.FORCE_BCF));

		writer.writeHeader(header);
		stopOut.stopped = false;

		this.header = header;
	}

	@Override public void close(TaskAttemptContext ctx) throws IOException {
		writer.close();
	}

	protected void writeRecord(VariantContext vc) {
		final GenotypesContext gc = vc.getGenotypes();
		if (gc instanceof LazyParsingGenotypesContext)
			((LazyParsingGenotypesContext)gc).getParser().setHeader(this.header);

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
//
// In addition we do BGZF compression here, to simplify things.
final class BCFStoppableOutputStream extends FilterOutputStream {
	public boolean stopped;
	private final OutputStream origOut;

	public BCFStoppableOutputStream(boolean startStopped, OutputStream out) {
		super(new BlockCompressedOutputStream(out, null));
		origOut = out;
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

	@Override public void close() throws IOException {
		// Don't close the BlockCompressedOutputStream, as we don't want
		// the BGZF terminator.
		this.out.flush();

		// Instead, close the lower-level output stream directly.
		origOut.close();
	}
}
