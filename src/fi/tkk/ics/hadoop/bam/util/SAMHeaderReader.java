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

// File created: 2013-07-26 13:54:32

package fi.tkk.ics.hadoop.bam.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;

public final class SAMHeaderReader {
	public static SAMFileHeader readSAMHeaderFrom(Path path, Configuration conf)
		throws IOException
	{
		InputStream i = path.getFileSystem(conf).open(path);
		final SAMFileHeader h = readSAMHeaderFrom(i, conf);
		i.close();
		return h;
	}

	/** Does not close the stream. */
	public static SAMFileHeader readSAMHeaderFrom(
		final InputStream in, final Configuration conf)
	{
		return new SAMFileReader(in, false).getFileHeader();
	}
}
