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

// File created: 2013-07-03 14:59:14

package org.seqdoop.hadoop_bam;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.lang.reflect.Array;

import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFHeader;

// See the comment in VariantContextWritable explaining what this is used for.
public final class VariantContextCodec {
	public static void write(final DataOutput out, final VariantContext vc)
		throws IOException
	{
        Object genotypesData;
        int numGenotypes;
        if (vc.getGenotypes().isLazyWithData()) {
            final htsjdk.variant.variantcontext.LazyGenotypesContext gc =
                    (htsjdk.variant.variantcontext.LazyGenotypesContext)
                            vc.getGenotypes();

            genotypesData = gc.getUnparsedGenotypeData();
            numGenotypes = gc.size();
        }
        else if (vc instanceof VariantContextWithHeader) {

            final VCFHeader header = ((VariantContextWithHeader)vc).getHeader();

            if (header == null) {
                throw new IllegalStateException( "Header not set inside VariantContextWithHeader" );
            }

            final List<String> genotypeAttributeKeys = vc.calcVCFGenotypeKeys(header);
            final StringBuilder builder = new StringBuilder();
            if ( ! genotypeAttributeKeys.isEmpty()) {
                // TODO: the VCFEncoder equivalent of this code checks for missing header fields here.  do we care?

                final String genotypeFormatString = ParsingUtils.join(VCFConstants.GENOTYPE_FIELD_SEPARATOR, genotypeAttributeKeys);

                builder.append(VCFConstants.FIELD_SEPARATOR);
                builder.append(genotypeFormatString);

                final VCFEncoder encoder = new VCFEncoder(header, true, false);
                final Map<Allele, String> alleleStrings = encoder.buildAlleleStrings(vc);
                encoder.addGenotypeData(vc, alleleStrings, genotypeAttributeKeys, builder);
            }
            genotypesData = builder.toString();
            numGenotypes = vc.getGenotypes().size();
        }
        else {
            throw new IllegalStateException( "Cannot write fully decoded VariantContext: need lazy genotypes or VCF Header" );
        }

		if (!(genotypesData instanceof String || genotypesData instanceof BCF2Codec.LazyData))
			throw new IllegalStateException(
				"Unrecognized unparsed genotype data, expected String or "+
				"BCF2Codec.LazyData: "+ genotypesData.getClass());

		final byte[] chrom = vc.getContig().getBytes("UTF-8");
		out.writeInt(chrom.length);
		out.write   (chrom);

		out.writeInt(vc.getStart());
		out.writeInt(vc.getEnd());

		final byte[] id = vc.getID().getBytes("UTF-8");
		out.writeInt(id.length);
		out.write   (id);

		final List<Allele> alleles = vc.getAlleles();
		out.writeInt(alleles.size());
		for (final Allele allele : alleles) {
			final byte[] b = allele.getDisplayBases();
			out.writeInt(b.length);
			out.write   (b);
		}

		if (vc.hasLog10PError())
			out.writeFloat((float)vc.getLog10PError());
		else {
			// The "missing value" used in BCF2, a signaling NaN.
			out.writeInt(0x7f800001);
		}

		if (vc.isFiltered()) {
			final Set<String> filters = vc.getFilters();
			out.writeInt(filters.size());
			for (final String s : filters) {
				final byte[] b = s.getBytes("UTF-8");
				out.writeInt(b.length);
				out.write   (b);
			}
		} else
			out.writeInt(vc.filtersWereApplied() ? -1 : -2);

		final Map<String,Object> attrs = vc.getAttributes();
		out.writeInt(attrs.size());
		for (final Map.Entry<String,Object> ent : attrs.entrySet()) {
			final byte[] k = ent.getKey().getBytes("UTF-8");
			out.writeInt(k.length);
			out.write   (k);

			encodeAttrVal(out, ent.getValue());
		}

		out.writeInt(numGenotypes);

		if (genotypesData instanceof String) {
			out.writeByte(0);
			final byte[] genob = ((String)genotypesData).getBytes("UTF-8");
			out.writeInt(genob.length);
			out.write   (genob);
		} else {
			assert genotypesData instanceof BCF2Codec.LazyData;
			final BCF2Codec.LazyData data = (BCF2Codec.LazyData)genotypesData;
			out.writeByte(1);
			out.writeInt(data.bytes.length);
			out.write   (data.bytes);
			out.writeInt(data.nGenotypeFields);
		}
	}

	public static VariantContext read(final DataInput in) throws IOException {
		final VariantContextBuilder builder = new VariantContextBuilder();

		int count, len;
		byte[] b;

		len = in.readInt();
		b = new byte[len];
		in.readFully(b);
		final String chrom = new String(b, "UTF-8");
		builder.chr(chrom);

		final int start = in.readInt();
		builder.start(start);
		builder.stop (in.readInt());

		len = in.readInt();
		if (len == 0)
			builder.noID();
		else {
			if (len > b.length) b = new byte[len];
			in.readFully(b, 0, len);
			builder.id(new String(b, 0, len, "UTF-8"));
		}

		count = in.readInt();
		final List<Allele> alleles = new ArrayList<Allele>(count);
		for (int i = 0; i < count; ++i) {
			len = in.readInt();
			if (len > b.length) b = new byte[len];
			in.readFully(b, 0, len);
			alleles.add(Allele.create(Arrays.copyOf(b, len), i == 0));
		}
		builder.alleles(alleles);

		final int qualInt = in.readInt();
		builder.log10PError(
			qualInt == 0x7f800001
				? VariantContext.NO_LOG10_PERROR
				: Float.intBitsToFloat(qualInt));

		count = in.readInt();
		switch (count) {
		case -2: builder.unfiltered(); break;
		case -1: builder.passFilters(); break;
		default:
			while (count-- > 0) {
				len = in.readInt();
				if (len > b.length) b = new byte[len];
				in.readFully(b, 0, len);
				builder.filter(new String(b, 0, len, "UTF-8"));
			}
			break;
		}

		count = in.readInt();
		final Map<String,Object> attrs = new HashMap<String,Object>(count, 1);
		while (count-- > 0) {
			len = in.readInt();
			if (len > b.length) b = new byte[len];
			in.readFully(b, 0, len);
			attrs.put(new String(b, 0, len, "UTF-8"), decodeAttrVal(in));
		}
		builder.attributes(attrs);

		count = in.readInt();
		final byte genoType = in.readByte();
		len = in.readInt();

		// Resize b even if it's already big enough, minimizing the amount of
		// memory LazyGenotypesContext hangs on to.
		b = new byte[len];
		in.readFully(b);

		switch (genoType) {
		case 0:
			builder.genotypesNoValidation(
				new LazyVCFGenotypesContext(alleles, chrom, start, b, count));
			break;

		case 1:
			builder.genotypesNoValidation(
				new LazyBCFGenotypesContext(alleles, in.readInt(), b, count));
			break;

		default:
			throw new IOException(
				"Invalid genotypes type identifier: cannot decode");
		}

		return builder.make();
	}

	// The VCF 4.1 spec says: "Integer, Float, Flag, Character, and String". But
	// there can be many, so we also have ARRAY.
	//
	// In addition, VariantContext seems to represent some/all floats as doubles
	// at least when reading from BCF, and at least BCF2FieldEncoder assumes
	// them to be of class Double so we have to preserve doubles and thus must
	// have DOUBLE.
	private enum AttrType {
		INT, FLOAT, BOOL, CHAR, STRING, ARRAY, DOUBLE;

		public byte toByte() { return (byte)ordinal(); }

		private static final AttrType[] values = values();
		public static AttrType fromByte(byte b) { return values[b]; }
	}

	private static void encodeAttrVal(final DataOutput out, final Object v)
		throws IOException
	{
		if (v instanceof Integer) {
			out.writeByte(AttrType.INT.toByte());
			out.writeInt ((Integer)v);
		} else if (v instanceof Float) {
			out.writeByte (AttrType.FLOAT.toByte());
			out.writeFloat((Float)v);
		} else if (v instanceof Double) {
			out.writeByte  (AttrType.DOUBLE.toByte());
			out.writeDouble((Double)v);
		} else if (v instanceof Boolean) {
			out.writeByte   (AttrType.BOOL.toByte());
			out.writeBoolean((Boolean)v);
		} else if (v instanceof Character) {
			out.writeByte(AttrType.CHAR.toByte());
			out.writeChar((Character)v);

		} else if (v instanceof List) {
			encodeAttrVal(out, ((List)v).toArray());

		} else if (v != null && v.getClass().isArray()) {
			out.writeByte(AttrType.ARRAY.toByte());
			final int length = Array.getLength(v);
			out.writeInt(length);
			for (int i = 0; i < length; ++i)
				encodeAttrVal(out, Array.get(v, i));

		} else {
			out.writeByte(AttrType.STRING.toByte());
			if (v == null)
				out.writeInt(0);
			else {
				final byte[] b = v.toString().getBytes("UTF-8");
				out.writeInt(b.length);
				out.write   (b);
			}
		}
	}

	private static Object decodeAttrVal(final DataInput in) throws IOException {
		switch (AttrType.fromByte(in.readByte())) {
			case INT:    return in.readInt();
			case FLOAT:  return in.readFloat();
			case DOUBLE: return in.readDouble();
			case BOOL:   return in.readBoolean();
			case CHAR:   return in.readChar();
			case ARRAY: {
				// VariantContext.fullyDecodeAttributes() checks for "instanceof
				// List" so we have to return a List, not an array, here.
				int len = in.readInt();
				final List<Object> os = new ArrayList<Object>(len);
				while (len-- > 0)
					os.add(decodeAttrVal(in));
				return os;
			}
			case STRING: {
				final int len = in.readInt();
				if (len == 0)
					return null;
				final byte[] b = new byte[len];
				in.readFully(b);
				return new String(b, "UTF-8");
			}
		}
		assert (false);
		throw new IOException("Invalid type identifier: cannot decode");
	}
}
