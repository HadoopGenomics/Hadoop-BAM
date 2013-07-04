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

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.broad.tribble.TribbleException;
import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.variantcontext.Genotype;
import org.broadinstitute.variant.variantcontext.GenotypeBuilder;
import org.broadinstitute.variant.variantcontext.LazyGenotypesContext;
import org.broadinstitute.variant.bcf2.BCF2Codec;
import org.broadinstitute.variant.bcf2.BCF2Decoder;
import org.broadinstitute.variant.bcf2.BCF2GenotypeFieldDecoders;
import org.broadinstitute.variant.bcf2.BCF2Utils;
import org.broadinstitute.variant.vcf.VCFHeader;

// XXX: Since we cannot use BCF2LazyGenotypesDecoder, the parsing functionality
//      is, unfortunately, simply copied from there.
public class LazyBCFGenotypesContext extends LazyParsingGenotypesContext {

	/** Takes ownership of the given byte[]: don't modify its contents. */
	public LazyBCFGenotypesContext(
		List<Allele> alleles, int fields, byte[] unparsed, int count)
	{
		super(new Parser(alleles, fields), unparsed, count);
	}

	public static class Parser extends LazyParsingGenotypesContext.Parser {
		private VCFHeader header = null;
		private final List<Allele> alleles;
		private final int fields;

		private final BCF2Decoder decoder = new BCF2Decoder();

		private BCF2GenotypeFieldDecoders genoFieldDecoders;
		private List<String> fieldDict;
		private GenotypeBuilder[] builders;

		public Parser(List<Allele> alleles, int fields) {
			this.alleles = alleles;
			this.fields = fields;
		}

		@Override public void setHeader(VCFHeader header) {
			this.header = header;

			genoFieldDecoders = new BCF2GenotypeFieldDecoders(header);
			fieldDict = BCF2Utils.makeDictionary(header);

			builders = new GenotypeBuilder[header.getNGenotypeSamples()];
			final List<String> genotypeSamples = header.getGenotypeSamples();
			for (int i = 0; i < builders.length; ++i)
				builders[i] = new GenotypeBuilder(genotypeSamples.get(i));
		}

		@Override public LazyGenotypesContext.LazyData parse(final Object data) {
			if (header == null)
				throw new IllegalStateException(
					"Cannot decode genotypes without a VCFHeader");

			// The following is essentially the contents of
			// BCF2LazyGenotypesDecoder.parse().

			try {
				decoder.setRecordBytes((byte[])data);

				for (final GenotypeBuilder gb : builders)
					gb.reset(true);

				for (int i = 0; i < fields; ++i) {
					final String field =
						fieldDict.get((Integer)decoder.decodeTypedValue());

					final byte typeDesc = decoder.readTypeDescriptor();
					final int numElems = decoder.decodeNumberOfElements(typeDesc);

					genoFieldDecoders.getDecoder(field).decode(
						alleles, field, decoder, typeDesc, numElems, builders);
				}

				final ArrayList<Genotype> genotypes =
					new ArrayList<Genotype>(builders.length);
				for (final GenotypeBuilder gb : builders)
					genotypes.add(gb.make());

				return new LazyGenotypesContext.LazyData(
					genotypes,
					header.getSampleNamesInOrder(), header.getSampleNameToOffset());
			} catch (IOException e) {
            throw new TribbleException(
            	"Unexpected IOException parsing genotypes data block", e);
			}
		}
	}
}
