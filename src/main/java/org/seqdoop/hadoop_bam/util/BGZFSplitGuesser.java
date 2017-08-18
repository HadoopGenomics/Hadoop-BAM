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

import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import htsjdk.samtools.util.BlockCompressedInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

public class BGZFSplitGuesser {
	private InputStream inFile;
	private Seekable seekableInFile;
	private       ByteArraySeekableStream in;
	private final ByteBuffer buf;
    private String name = null;
    
    private final static int BGZF_MAGIC_0 = 0x1f;
    private final static int BGZF_MAGIC_1 = 0x8b;
    private final static int BGZF_MAGIC_2 = 0x08;
    private final static int BGZF_MAGIC_3 = 0x04;
    private final static int BGZF_MAGIC = 0x04088b1f;
    private final static int BGZF_MAGIC_SUB = 0x00024342;

	public BGZFSplitGuesser(InputStream is) {
		inFile = is;
		seekableInFile = (Seekable) is;

		buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.LITTLE_ENDIAN);
	}

    public void setName(String _name) {
        name = _name;
    }

	public BGZFSplitGuesser(FSDataInputStream is) {
		inFile = is;
		seekableInFile = is;

		buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.LITTLE_ENDIAN);
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

		this.seekableInFile.seek(beg);
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

		for (int pos = 0;;) {
			pos = guessNextBGZFPos(pos, firstBGZFEnd);
			if (pos < 0)
				return end;

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

	// Returns a negative number if it doesn't find anything.
	private int guessNextBGZFPos(int p, int end)
		throws IOException
	{
            while(true) {
                boolean found_block_start = false;
                boolean in_magic = false;
                in.seek(p);
                while(!found_block_start) {
                    int n = in.read();

                    if (n == BGZF_MAGIC_0) {
                        in_magic = true;
                    } else if (n == BGZF_MAGIC_3 && in_magic) {
                        found_block_start = true;
                    } else if (p >= end) {
                        return -1;
                    } else if (!((n == BGZF_MAGIC_1 && in_magic) ||
                                 (n == BGZF_MAGIC_2 && in_magic))) {
                        in_magic = false;
                    }
                    p++;
                }

                // after the magic number:
                // skip 6 unspecified bytes (MTIME, XFL, OS)
                // XLEN = 6 (little endian, so 0x0600)
                // SI1  = 0x42
                // SI2  = 0x43
                // SLEN = 0x02
                in.seek(p + 6);
                int n = in.read();
                if (0x06 != n) {
                    continue;
                }
                n = in.read();
                if (0x00 != n) {
                    continue;
                }
                n = in.read();
                if (0x42 != n) {
                    continue;
                }
                n = in.read();
                if (0x43 != n) {
                    continue;
                }
                n = in.read();
                if (0x02 != n) {
                    continue;
                }         

                return p - 4;
            }
	}
}
