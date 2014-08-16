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

// File created: 2013-07-05 15:48:33

package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.bcf2.BCF2Decoder;
import htsjdk.variant.bcf2.BCF2GenotypeFieldDecoders;
import htsjdk.variant.bcf2.BCF2Utils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.vcf.VCFHeader;

// XXX: Since we cannot use BCF2LazyGenotypesDecoder, the parsing functionality
//      is, unfortunately, simply copied from there.
public class LazyBCFGenotypesContext extends LazyParsingGenotypesContext {

	/** Takes ownership of the given byte[]: don't modify its contents. */
	public LazyBCFGenotypesContext(
		List<Allele> alleles, int fields, byte[] unparsed, int count)
	{
		super(new Parser(alleles, fields), unparsed, count);
	}

	public static class HeaderDataCache
		implements LazyParsingGenotypesContext.HeaderDataCache
	{
		public static final BCF2Decoder decoder = new BCF2Decoder();

		private BCF2GenotypeFieldDecoders genoFieldDecoders;
		private List<String>              fieldDict;
		private GenotypeBuilder[]         builders;

		private ArrayList<String>        sampleNamesInOrder;
		private HashMap<String, Integer> sampleNameToOffset;

		@Override public void setHeader(VCFHeader header) {
			genoFieldDecoders = new BCF2GenotypeFieldDecoders(header);
			fieldDict = BCF2Utils.makeDictionary(header);

			builders = new GenotypeBuilder[header.getNGenotypeSamples()];
			final List<String> genotypeSamples = header.getGenotypeSamples();
			for (int i = 0; i < builders.length; ++i)
				builders[i] = new GenotypeBuilder(genotypeSamples.get(i));

			sampleNamesInOrder = header.getSampleNamesInOrder();
			sampleNameToOffset = header.getSampleNameToOffset();
		}

		public BCF2GenotypeFieldDecoders getGenoFieldDecoders() {
			return genoFieldDecoders;
		}
		public List<String>      getFieldDict() { return fieldDict; }
		public GenotypeBuilder[] getBuilders () { return builders; }

		public ArrayList<String> getSampleNamesInOrder() {
			return sampleNamesInOrder;
		}
		public HashMap<String, Integer> getSampleNameToOffset() {
			return sampleNameToOffset;
		}
	}

	public static class Parser extends LazyParsingGenotypesContext.Parser {
		private final List<Allele> alleles;
		private final int fields;

		private HeaderDataCache hd = null;

		public Parser(List<Allele> alleles, int fields) {
			this.alleles = alleles;
			this.fields = fields;
		}

		@Override public void setHeaderDataCache(
			LazyParsingGenotypesContext.HeaderDataCache data)
		{
			this.hd = (HeaderDataCache)data;
		}

		@Override public LazyGenotypesContext.LazyData parse(final Object data) {
			if (hd == null)
				throw new IllegalStateException(
					"Cannot decode genotypes without HeaderDataCache");

			final GenotypeBuilder[] builders = hd.getBuilders();

			// The following is essentially the contents of
			// BCF2LazyGenotypesDecoder.parse().

			try {
				hd.decoder.setRecordBytes((byte[])data);

				for (final GenotypeBuilder gb : builders)
					gb.reset(true);

				for (int i = 0; i < fields; ++i) {
					final String field =
						hd.getFieldDict().get(
							(Integer)hd.decoder.decodeTypedValue());

					final byte type = hd.decoder.readTypeDescriptor();
					final int numElems = hd.decoder.decodeNumberOfElements(type);

					hd.getGenoFieldDecoders().getDecoder(field).decode(
						alleles, field, hd.decoder, type, numElems, builders);
				}

				final ArrayList<Genotype> genotypes =
					new ArrayList<Genotype>(builders.length);
				for (final GenotypeBuilder gb : builders)
					genotypes.add(gb.make());

				return new LazyGenotypesContext.LazyData(
					genotypes,
					hd.getSampleNamesInOrder(), hd.getSampleNameToOffset());
			} catch (IOException e) {
            throw new TribbleException(
            	"Unexpected IOException parsing genotypes data block", e);
			}
		}
	}
}
