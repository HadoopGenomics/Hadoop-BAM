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

package org.seqdoop.hadoop_bam;

import java.io.UnsupportedEncodingException;
import java.util.List;

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.vcf.AbstractVCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderVersion;

// File created: 2013-07-03 15:41:21

// The actual parsing is delegated to AbstractVCFCodec.
public class LazyVCFGenotypesContext extends LazyParsingGenotypesContext {

	/** Takes ownership of the given byte[]: don't modify its contents. */
	public LazyVCFGenotypesContext(
		List<Allele> alleles, String chrom, int start,
		byte[] utf8Unparsed, int count)
	{
		super(new Parser(alleles, chrom, start), utf8Unparsed, count);
	}

	public static class HeaderDataCache
		implements LazyParsingGenotypesContext.HeaderDataCache
	{
		private HeaderSettableVCFCodec codec = new HeaderSettableVCFCodec();

		@Override public void setHeader(VCFHeader header) {
			VCFHeaderVersion version = null;

			// Normally AbstractVCFCodec parses the header and thereby sets the
			// version field. It gets used later on so we need to set it.
			for (final VCFHeaderLine line : header.getMetaDataInInputOrder()) {
				if (VCFHeaderVersion.isFormatString(line.getKey())) {
					version = VCFHeaderVersion.toHeaderVersion(line.getValue());
					break;
				}
			}

			codec.setHeaderAndVersion(header, version);
		}

		public AbstractVCFCodec getCodec() { return codec; }
	}

	public static class Parser extends LazyParsingGenotypesContext.Parser {
		private HeaderSettableVCFCodec codec = null;
		private final List<Allele> alleles;
		private final String chrom;
		private final int start;

		public Parser(List<Allele> alleles, String chrom, int start) {
			this.alleles = alleles;
			this.chrom = chrom;
			this.start = start;
		}

		@Override public void setHeaderDataCache(
			LazyParsingGenotypesContext.HeaderDataCache data)
		{
			codec = (HeaderSettableVCFCodec)((HeaderDataCache)data).getCodec();
		}

		@Override public LazyGenotypesContext.LazyData parse(final Object data) {
			if (codec == null || !codec.hasHeader())
				throw new IllegalStateException(
					"Cannot decode genotypes without a codec with a VCFHeader");

			final String str;
			try {
				str = new String((byte[])data, "UTF-8");
			} catch (UnsupportedEncodingException absurd) {
				throw new RuntimeException(
					"Can never happen on a compliant Java implementation because "+
					"UTF-8 is guaranteed to be supported");
			}
			return codec.createGenotypeMap(str, alleles, chrom, start);
		}
	}
}

// This is a HACK. But, the functionality is only in AbstractVCFCodec so it
// can't be helped. This is preferable to copying the functionality into
// parse() above.
class HeaderSettableVCFCodec extends AbstractVCFCodec {
	public boolean hasHeader() { return header != null; }

	public void setHeaderAndVersion(VCFHeader header, VCFHeaderVersion ver) {
		this.header = header;
		this.version = ver;
	}

	@Override public Object readActualHeader(LineIterator reader) {
		throw new UnsupportedOperationException(
			"Internal error: this shouldn't be called");
	}
	@Override public List<String> parseFilters(String filterString) {
		throw new UnsupportedOperationException(
			"Internal error: this shouldn't be called");
	}
	@Override public boolean canDecode(String s) {
		return true;
	}
}
