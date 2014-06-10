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

// File created: 2011-04-18 14:46:31

package org.seqdoop.hadoopbam.util;

import java.io.IOException;

import net.sf.samtools.seekablestream.SeekableStream;

public class SeekableArrayStream extends SeekableStream {
	private final byte[] arr;
	private       int    pos;

	public SeekableArrayStream(byte[] a) { this.arr = a; this.pos = 0; }

	@Override public void    close   () {}
	@Override public long    length  () { return arr.length; }
	@Override public long    position() { return pos; }
	@Override public boolean eof     () { return pos == length(); }

	@Override public void seek(long lp) throws IOException {
		final int p = (int)lp;
		if (p < 0 || p > arr.length)
			throw new IOException("position " +p+ " is out of bounds");
		pos = p;
	}

	@Override public int read(byte[] b, int off, int len) {
		if (eof())
			return -1;
		len = Math.min(len, arr.length - pos);
		System.arraycopy(arr, pos, b, off, len);
		pos += len;
		return len;
	}
	@Override public int read() { return eof() ? -1 : arr[pos++]; }

	@Override public String getSource() { return null; }
}
