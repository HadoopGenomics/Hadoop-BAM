// Copyright (c) 2011 Aalto University
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

// File created: 2011-05-31 11:40:06

package org.seqdoop.hadoop_bam.util;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import htsjdk.samtools.util.BlockCompressedInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

import org.seqdoop.hadoop_bam.BaseSplitGuesser;

public class BGZFSplitGuesser extends BaseSplitGuesser {

    private       SeekableStream             inFile;

	public BGZFSplitGuesser(SeekableStream is) {
		inFile = is;
	}

	/// Looks in the range [beg,end). Returns end if no BAM record was found.
	public long guessNextBGZFBlockStart(long beg, long end)
		throws IOException
	{
		// Buffer what we need to go through. Since the max size of a BGZF block
		// is 0xffff (64K), and we might be just one byte off from the start of
		// the previous one, we need 0xfffe bytes for the start, and then 0xffff
		// for the block we're looking for.

		byte[] arr = new byte[0xffff];

		this.inFile.seek(beg);
                int bytesToRead = Math.min((int) (end - beg), arr.length);
                int totalRead = 0;
                // previously, this code did a single read and assumed that if it did not
                // return an error code, then it had filled the array. however, when an
                // incomplete copy occurs, the split picker will try to read past the end
                // of the bucket, which will lead to the split picker returning an error
                // code (-1), which gets mishandled elsewhere...
                while(totalRead < bytesToRead) {
                    int read = inFile.read(arr, totalRead, bytesToRead - totalRead);
                    if (read == -1) {
			return -1; // EOF
                    }
                    totalRead += read;
                }
		arr = Arrays.copyOf(arr, totalRead);

		this.in = new ByteArraySeekableStream(arr);

		final BlockCompressedInputStream bgzf =
			new BlockCompressedInputStream(this.in);
		bgzf.setCheckCrcs(true);

		final int firstBGZFEnd = Math.min((int)(end - beg), 0xffff);

                PosSize p;
		for (int pos = 0;;) {
			p = guessNextBGZFPos(pos, firstBGZFEnd);
			if (p == null)
				return end;
                        pos = p.pos;
                        
			try {
				// Seek in order to trigger decompression of the block and a CRC
				// check.
				bgzf.seek((long)pos << 16);

			// This has to catch Throwable, because it's possible to get an
			// OutOfMemoryError due to an overly large size.
			} catch (Throwable e) {
				// Guessed BGZF position incorrectly: try the next guess.
				++pos;
				continue;
			}
			return beg + pos;
		}
	}
}
