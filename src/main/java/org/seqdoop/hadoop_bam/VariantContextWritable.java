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

// File created: 2013-06-26 10:27:20

package org.seqdoop.hadoop_bam;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

/** VariantContexts read here have LazyGenotypesContexts, which need to have a
 * header set before the genotype data in the VariantContexts can be decoded.
 * See the LazyGenotypesContext class.
 */
public class VariantContextWritable implements Writable {
	private VariantContext vc;

	public VariantContext get()                  { return vc; }
	public void           set(VariantContext vc) { this.vc = vc; }
    public void           set(VariantContext vc, VCFHeader header) { this.vc = new VariantContextWithHeader(vc, header); }

	// XXX: Unfortunately there's no simple way to just pass a BCF record
	// through. Contrasting to BAM, there's no equivalent of the BAMRecord
	// subclass of SAMRecord that saves the original BAM fields --- a
	// VariantContext only saves the decoded info, so it's impossible to encode
	// one to BCF without the header.
	//
	// VCF is also unusable because VCFWriter defensively refuses to write
	// anything without a header, throwing IllegalStateException if attempted.
	//
	// Thus, we have a custom encoding.
	@Override public void write(final DataOutput out) throws IOException {
		VariantContextCodec.write(out, vc);
	}
	@Override public void readFields(final DataInput in) throws IOException {
		vc = VariantContextCodec.read(in);
	}
}
