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

// File created: 2011-06-14 13:38:57

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.SAMFileWriterImpl;
import net.sf.samtools.SAMFormatException;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMTextWriter;
import net.sf.samtools.util.SeekableStream;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;

import fi.tkk.ics.hadoop.bam.SAMFormat;
import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

public final class View extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		headerOnlyOpt = new BooleanOption('H', "header-only"),
		formatOpt     = new StringOption ('F', "format=FMT");

	public View() {
		super("view", "SAM and BAM viewing", "1.1", "PATH [regions...]",
			optionDescs,
			"Reads the BAM or SAM file in PATH and, by default, outputs it in "+
			"SAM format. If any number of regions is given, only the alignments "+
			"overlapping with those regions are output. Then an index is also "+
			"required, expected at PATH.bai by default."+
			"\n\n"+
			"Regions can be given as only reference sequence names or indices "+
			"like 'chr1', or with position ranges as well like 'chr1:100-200'. "+
			"These coordinates are 1-based, with 0 representing the start or "+
			"end of the sequence.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			headerOnlyOpt, "print header only"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			formatOpt, "select the output format based on FMT: SAM or BAM"));
	}

	@Override protected int run(CmdLineParser parser) {

		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("view :: PATH not given.");
			return 3;
		}
		final String       path    = args.get(0);
		final List<String> regions = args.subList(1, args.size());

		final boolean headerOnly = parser.getBoolean(headerOnlyOpt);

		final SAMFileReader reader;

		try {
			final Path p = new Path(path);

			SeekableStream idx;
			try {
				idx = WrapSeekable.openPath(getConf(), p.suffix(".bai"));
			} catch (Exception e) {
				idx = null;
			}

			final SeekableStream sam = WrapSeekable.openPath(getConf(), p);

			reader = idx == null ? new SAMFileReader(sam,      false)
			                     : new SAMFileReader(sam, idx, false);
		} catch (Exception e) {
			System.err.printf("view :: Could not open '%s': %s\n",
			                  path, e.getMessage());
			return 4;
		}

		reader.setValidationStringency(ValidationStringency.SILENT);

		final SAMFileHeader header;

		try {
			header = reader.getFileHeader();
		} catch (SAMFormatException e) {
			System.err.printf("view :: Could not parse '%s': %s\n",
			                  path, e.getMessage());
			return 4;
		}

		final String fmt = (String)parser.getOptionValue(formatOpt);

		final SAMFormat format =
			fmt == null ? SAMFormat.SAM
			            : SAMFormat.valueOf(fmt.toUpperCase(Locale.ENGLISH));

		final SAMFileWriterImpl writer;
		switch (format) {
			case BAM:
				writer = new BAMFileWriter(System.out, new File("<stdout>"));
				break;
			case SAM:
				writer = new SAMTextWriter(System.out);
				break;
			default: writer = null; assert false;
		}

		if (regions.isEmpty() || headerOnly) {
			writer.setSortOrder(header.getSortOrder(), true);
			writer.setHeader(header);

			if (!headerOnly)
				if (!writeIterator(writer, reader.iterator(), path))
					return 4;

			writer.close();
			return 0;
		}

		if (!reader.isBinary()) {
			System.err.println("view :: Cannot output regions from SAM file");
			return 4;
		}

		if (!reader.hasIndex()) {
			System.err.println(
				"view :: Cannot output regions from BAM file lacking an index");
			return 4;
		}

		reader.enableIndexCaching(true);

		boolean errors = false;

		for (final String region : regions) {
			final StringTokenizer st = new StringTokenizer(region, ":-");
			final String refStr = st.nextToken();
			final int beg, end;

			if (st.hasMoreTokens()) {
				beg = parseCoordinate(st.nextToken());
				end = st.hasMoreTokens() ? parseCoordinate(st.nextToken()) : -1;

				if (beg < 0 || end < 0) {
					errors = true;
					continue;
				}
				if (end < beg) {
					System.err.printf(
						"view :: Invalid range, cannot end before start: '%d-%d'\n",
						beg, end);
					errors = true;
					continue;
				}
			} else
				beg = end = 0;

			SAMSequenceRecord ref = header.getSequence(refStr);
			if (ref == null) try {
				ref = header.getSequence(Integer.parseInt(refStr));
			} catch (NumberFormatException e) {}

			if (ref == null) {
				System.err.printf(
					"view :: Not a valid sequence name or index: '%s'\n", refStr);
				errors = true;
				continue;
			}

			final SAMRecordIterator it =
				reader.queryOverlapping(ref.getSequenceName(), beg, end);

			if (!writeIterator(writer, it, path))
				return 4;
		}
		writer.close();
		return errors ? 5 : 0;
	}

	private boolean writeIterator(
		SAMFileWriterImpl writer, SAMRecordIterator it, String path)
	{
		try {
			while (it.hasNext())
				writer.addAlignment(it.next());
			return true;
		} catch (SAMFormatException e) {
			writer.close();
			System.err.printf("view :: Could not parse '%s': %s\n",
			                  path, e.getMessage());
			return false;
		}
	}

	private int parseCoordinate(String s) {
		int c;
		try {
			c = Integer.parseInt(s);
		} catch (NumberFormatException e) {
			c = -1;
		}
		if (c < 0)
			System.err.printf("view :: Not a valid coordinate: '%s'\n", s);
		return c;
	}
}
