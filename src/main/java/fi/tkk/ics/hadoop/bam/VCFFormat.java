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

// File created: 2013-06-27 13:21:07

package org.seqdoop.hadoopbam;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

/** Describes a VCF format. */
public enum VCFFormat {
	VCF, BCF;

	/** Infers the VCF format by looking at the filename of the given path.
	 *
	 * @see #inferFromFilePath(String)
	 */
	public static VCFFormat inferFromFilePath(final Path path) {
		return inferFromFilePath(path.getName());
	}

	/** Infers the VCF format by looking at the extension of the given file
	 * name. <code>*.vcf</code> is recognized as {@link #VCF} and
	 * <code>*.bcf</code> as {@link #BCF}.
	 */
	public static VCFFormat inferFromFilePath(final String name) {
		if (name.endsWith(".bcf")) return BCF;
		if (name.endsWith(".vcf")) return VCF;
		return null;
	}

	public static VCFFormat inferFromData(final InputStream in) throws IOException {
		final byte b = (byte)in.read();
		in.close();
		switch (b) {
			case 0x1f: return BCF; // BGZF-compressed
			case 'B':  return BCF; // Not compressed
			case '#':  return VCF;
		}
		return null;
	}
}
