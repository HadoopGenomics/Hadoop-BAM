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

// File created: 2013-07-04 10:49:20

package org.seqdoop.hadoop_bam.util;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;

import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.readers.PositionalBufferedStream;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import java.util.zip.GZIPInputStream;
import org.seqdoop.hadoop_bam.VCFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Can read a VCF header without being told beforehand whether the input is
 * VCF or BCF.
 */
public final class VCFHeaderReader {
	private static final Logger logger = LoggerFactory.getLogger(VCFHeaderReader.class);

	public static VCFHeader readHeaderFrom(final SeekableStream in)
		throws IOException
	{
		Object headerCodec = null;
        Object header = null;
		final long initialPos = in.position();
		try {
			BufferedInputStream bis = new BufferedInputStream(in);
			InputStream is = VCFFormat.isGzip(bis) ? new GZIPInputStream(bis) : bis;
			headerCodec = new VCFCodec().readHeader(new AsciiLineReaderIterator(new AsciiLineReader(is)));
		} catch (TribbleException e) {
			logger.warn("Exception while trying to read VCF header from file:", e);

			in.seek(initialPos);

			InputStream bin = new BufferedInputStream(in);
			if (BlockCompressedInputStream.isValidFile(bin))
				bin = new BlockCompressedInputStream(bin);

			headerCodec =
				new BCF2Codec().readHeader(
					new PositionalBufferedStream(bin));
		}
		if (!(headerCodec instanceof FeatureCodecHeader))
			throw new IOException("No VCF header found");
        header = ((FeatureCodecHeader)headerCodec).getHeaderValue();
		return (VCFHeader)header;
	}
}
