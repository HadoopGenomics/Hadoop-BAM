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

package org.seqdoop.hadoopbam.util;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;

import net.sf.samtools.seekablestream.SeekableStream;
import net.sf.samtools.util.BlockCompressedInputStream;

import org.broad.tribble.FeatureCodecHeader;
import org.broad.tribble.TribbleException;
import org.broad.tribble.readers.AsciiLineReader;
import org.broad.tribble.readers.AsciiLineReaderIterator;
import org.broad.tribble.readers.PositionalBufferedStream;
import org.broadinstitute.variant.bcf2.BCF2Codec;
import org.broadinstitute.variant.vcf.VCFCodec;
import org.broadinstitute.variant.vcf.VCFHeader;

/** Can read a VCF header without being told beforehand whether the input is
 * VCF or BCF.
 */
public final class VCFHeaderReader {
	public static VCFHeader readHeaderFrom(final SeekableStream in)
		throws IOException
	{
		Object headerCodec = null;
        Object header = null;
		final long initialPos = in.position();
		try {
			headerCodec = new VCFCodec().readHeader(new AsciiLineReaderIterator(new AsciiLineReader(in)));
		} catch (TribbleException e) {
            System.err.println("warning: while trying to read VCF header from file received exception: "+e.toString());

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
