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

// File created: 2013-07-05 16:18:57

package org.seqdoop.hadoop_bam;

import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.vcf.VCFHeader;

/** You need to call getParser().setHeader() here before trying to decode() a
 * GenotypesContext in any VariantContext that came about via
 * VariantContextWritable.readFields(). That includes calling
 * VariantContext.fullyDecode() or almost any of the GenotypesContext methods.
 * The RecordReader provided by VCFInputFormat does this for you.
 */
// There's no public LazyGenotypesContext.LazyParser in Picard so we need to
// provide our own. Since we need to have the header in the parser set
// externally, we also need to provide a LazyGenotypesContext which gives
// access to the parser.
//
// And since VCF and BCF have different kinds of lazy data, we have separate
// classes implementing the actual parsing for each.
public abstract class LazyParsingGenotypesContext
	extends LazyGenotypesContext
{
	// super.parser is inaccessible to us so we keep a copy that we can access.
	private final Parser parserCopy;

	protected LazyParsingGenotypesContext(Parser p, byte[] data, int count) {
		super(p, data, count);
		parserCopy = p;
	}

	public Parser getParser() { return parserCopy; }

	public static interface HeaderDataCache {
		public void setHeader(VCFHeader header);
	}

	public static abstract class Parser implements LazyParser {
		public abstract void setHeaderDataCache(HeaderDataCache data);
	}
}
