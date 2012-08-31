// Copyright (c) 2010 Aalto University
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

// File created: 2010-11-26 12:15:04

package fi.tkk.ics.hadoop.bam.util.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.sf.samtools.BAMIndex;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileSpan;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMTextWriter;
import net.sf.samtools.util.SeekableStream;

import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

public class BAMReader extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(null, new BAMReader(), args));
	}

	@Override public int run(String[] args)
		throws ClassNotFoundException, IOException
	{
		if (args.length < 2) {
			System.out.println(
				"Usage: BAMReader PATH referenceIndex:startPos-endPos [...]\n"+
				"\n"+
				"Reads the BAM file in PATH, expecting an index at PATH.bai, and "+
				"for each given\nregion, prints out the SAM records that overlap "+
				"with that region.\n"+
				"\n"+
				"PATH is a local file path if run outside Hadoop and an HDFS "+
				"path if run within\nit.");
			return args.length == 0 ? 0 : 2;
		}

		final Path path = new Path(args[0]);
		final FileSystem fs = path.getFileSystem(getConf());

		final SAMFileReader reader = new SAMFileReader(
			WrapSeekable.openPath(fs, path),
			WrapSeekable.openPath(fs, path.suffix(".bai")),
			false);

		reader.enableIndexCaching(true);

		final SAMFileHeader header = reader.getFileHeader();
		final BAMIndex      index  = reader.getIndex();

		final SAMTextWriter writer = new SAMTextWriter(System.out);

		boolean errors = false;

		for (final String arg : Arrays.asList(args).subList(1, args.length)) {
			final StringTokenizer st = new StringTokenizer(arg, ":-");
			final String refStr = st.nextToken();
			final int    beg    = parseCoordinate(st.nextToken());
			final int    end    = parseCoordinate(st.nextToken());

			if (beg < 0 || end < 0) {
				errors = true;
				continue;
			}

			int ref = header.getSequenceIndex(refStr);
			if (ref == -1) try {
				ref = Integer.parseInt(refStr);
			} catch (NumberFormatException e) {
				System.err.printf(
					"Not a valid sequence name or index: '%s'\n", refStr);
				errors = true;
				continue;
			}

			final SAMFileSpan span = index.getSpanOverlapping(ref, beg, end);

			if (span == null)
				continue;

			final SAMRecordIterator it = reader.iterator(span);

			while (it.hasNext())
				writer.writeAlignment(it.next());
		}
		writer.close();
		return errors ? 1 : 0;
	}

	private int parseCoordinate(String s) {
		int c;
		try {
			c = Integer.parseInt(s);
		} catch (NumberFormatException e) {
			c = -1;
		}
		if (c < 0)
			System.err.printf("Not a valid coordinate: '%s'\n", s);
		return c;
	}
}
