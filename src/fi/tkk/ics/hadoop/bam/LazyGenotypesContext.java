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

package fi.tkk.ics.hadoop.bam;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.broad.tribble.readers.LineReader;
import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.vcf.AbstractVCFCodec;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;

// File created: 2013-07-03 15:41:21

/** You need to call getParser().setHeader() here before trying to decode() a
 * GenotypesContext in any VariantContext that came about via
 * VariantContextWritable.readFields(). That includes calling
 * VariantContext.fullyDecode() or almost any of the GenotypesContext methods.
 */
// There's no public LazyGenotypesContext.LazyParser in Picard so we need to
// provide our own. Since we need to have the header in the parser set
// externally, we also need to provide a LazyGenotypesContext which gives
// access to the parser.
//
// The actual parsing is delegated to AbstractVCFCodec.
public class LazyGenotypesContext
	extends org.broadinstitute.variant.variantcontext.LazyGenotypesContext
{
	private final Parser parserCopy;

	/** Takes ownership of the given byte[]: don't modify its contents. */
	public LazyGenotypesContext(
		List<Allele> alleles, String chrom, int start,
		byte[] utf8Unparsed, int count)
	{
		this(new Parser(alleles, chrom, start), utf8Unparsed, count);
	}
	private LazyGenotypesContext(Parser p, byte[] utf8Unparsed, int count) {
		super(p, utf8Unparsed, count);
		parserCopy = p;
	}

	public Parser getParser() { return parserCopy; }

	public static class Parser implements LazyParser {
		private final List<Allele> alleles;
		private final String chrom;
		private final int start;

		private HeaderSettableVCFCodec codec = new HeaderSettableVCFCodec();

		public Parser(List<Allele> alleles, String chrom, int start) {
			this.alleles = alleles;
			this.chrom = chrom;
			this.start = start;
		}

		public void setHeader(VCFHeader header) { codec.setHeader(header); }

		@Override public LazyGenotypesContext.LazyData parse(final Object data) {
			if (!codec.hasHeader())
				throw new IllegalStateException(
					"Cannot decode genotypes without a VCFHeader");

			final String str;
			try {
				str = new String((byte[])data, "UTF-8");
			} catch (UnsupportedEncodingException absurd) {
				throw new RuntimeException(
					"Can never happen on a compliant Java implementation because "+
					"UTF-8 is guaranteed to be supported");
			}
			return
				Parser.this.codec.createGenotypeMap(str, alleles, chrom, start);
		}
	}
}

// This is a HACK. But, the functionality is only in AbstractVCFCodec so it
// can't be helped. This is preferable to copying the functionality into
// parse() above.
class HeaderSettableVCFCodec extends AbstractVCFCodec {
	public boolean hasHeader() { return header != null; }

	public void setHeader(VCFHeader header) {
		this.header = header;

		// Normally AbstractVCFCodec parses the header and thereby sets the
		// version field. It gets used later on so we need to set it.
		for (final VCFHeaderLine line : header.getMetaDataInInputOrder()) {
			if (VCFHeaderVersion.isFormatString(line.getKey())) {
				this.version = VCFHeaderVersion.toHeaderVersion(line.getValue());
				break;
			}
		}
	}

	@Override public Object readHeader(LineReader reader) {
		throw new UnsupportedOperationException(
			"Internal error: this shouldn't be called");
	}
	@Override public List<String> parseFilters(String filterString) {
		throw new UnsupportedOperationException(
			"Internal error: this shouldn't be called");
	}
}
