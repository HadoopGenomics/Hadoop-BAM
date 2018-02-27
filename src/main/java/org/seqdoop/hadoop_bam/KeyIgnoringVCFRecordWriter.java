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

// File created: 2013-06-27 09:42:56

package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import htsjdk.variant.vcf.VCFHeader;

/** A convenience class that you can use as a RecordWriter for VCF files.
 *
 * <p>The write function ignores the key, just outputting the
 * VariantContext.</p>
 */
public class KeyIgnoringVCFRecordWriter<K> extends VCFRecordWriter<K> {
	public KeyIgnoringVCFRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		super(output, input, writeHeader, ctx);
	}
	public KeyIgnoringVCFRecordWriter(
			Path output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		super(output, header, writeHeader, ctx);
	}
	/**
	 * @deprecated This constructor has no {@link TaskAttemptContext} so it is not
	 * possible to pass configuration properties to the writer.
	 */
	@Deprecated
	public KeyIgnoringVCFRecordWriter(
			OutputStream output, VCFHeader header, boolean writeHeader)
		throws IOException
	{
		super(output, header, writeHeader);
	}
	public KeyIgnoringVCFRecordWriter(
			OutputStream output, VCFHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
			throws IOException
	{
		super(output, header, writeHeader, ctx);
	}


	@Override public void write(K ignored, VariantContextWritable vc) {
		writeRecord(vc.get());
	}
}
