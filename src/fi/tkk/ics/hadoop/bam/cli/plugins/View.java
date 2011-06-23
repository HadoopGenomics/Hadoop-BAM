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
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

import net.sf.samtools.SAMFormatException;
import net.sf.samtools.SAMFileReader.ValidationStringency;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import fi.tkk.ics.hadoop.bam.custom.samtools.BAMIndex;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileHeader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileSpan;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecordIterator;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMTextWriter;

import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

public final class View extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		localFilesystemOpt = new BooleanOption('L', "--local-filesystem"),
		headerOnlyOpt      = new BooleanOption('H', "--header-only");

	public View() {
		super("view", "BAM viewing", "1.0", "PATH [regions...]", optionDescs,
			"Reads the BAM file in PATH and, by default, outputs it in SAM "+
			"format. If any number of regions is given, only the alignments "+
			"overlapping with those regions are output. Then an index is also "+
			"required, expected at PATH.bai by default."+
			"\n\n"+
			"By default, PATH is treated as a local file path if run outside "+
			"Hadoop and an HDFS path if run within it."+
			"\n\n"+
			"Regions can be given as only reference sequence names or indices "+
			"like 'chr1', or with position ranges as well like 'chr1:100-200'.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			localFilesystemOpt, "force use of the local FS instead of HDFS"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			headerOnlyOpt, "print header only"));
	}

	@Override protected int run(CmdLineParser parser) {

		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("view :: PATH not given.");
			return 3;
		}
		final String       path    = args.get(0);
		final List<String> regions = args.subList(1, args.size());

		final boolean
			localFilesystem = parser.getBoolean(localFilesystemOpt),
			headerOnly      = parser.getBoolean(headerOnlyOpt);

		final SAMFileReader reader;

		try {
			if (localFilesystem)
				reader = new SAMFileReader(
					new File(path), new File(path + ".bai"), false);
			else {
				final Path p = new Path(path);
				reader = new SAMFileReader(
					WrapSeekable.openPath(getConf(), p),
					WrapSeekable.openPath(getConf(), p.suffix(".bai")),
					false);
			}
		} catch (Exception e) {
			System.err.printf("view :: Could not open '%s': %s\n",
			                  path, e.getMessage());
			return 4;
		}

		// We'd rather get an exception than have Picard barf all the errors it
		// finds without us knowing about it.
		reader.setValidationStringency(ValidationStringency.STRICT);

		final SAMTextWriter writer = new SAMTextWriter(System.out);
		final SAMFileHeader header;

		try {
			header = reader.getFileHeader();
		} catch (SAMFormatException e) {
			System.err.printf("view :: Could not parse '%s': %s\n",
			                  path, e.getMessage());
			return 4;
		}

		if (regions.isEmpty() || headerOnly) {
			writer.setSortOrder(header.getSortOrder(), true);
			writer.setHeader(header);

			if (!headerOnly) try {
				final SAMRecordIterator it = reader.iterator();
				while (it.hasNext())
					writer.writeAlignment(it.next());
			} catch (SAMFormatException e) {
				writer.close();
				System.err.printf("view :: Could not parse '%s': %s\n",
				                  path, e.getMessage());
				return 4;
			}

			writer.close();
			return 0;
		}

		if (!reader.hasIndex()) {
			System.err.println(
				"view :: Cannot output regions from BAM file lacking an index");
			return 4;
		}

		reader.enableIndexCaching(true);
		final BAMIndex index = reader.getIndex();

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
			} else {
				beg = 0;
				end = SAMRecord.MAX_INSERT_SIZE;
			}

			int ref = header.getSequenceIndex(refStr);
			if (ref == -1) try {
				ref = Integer.parseInt(refStr);
			} catch (NumberFormatException e) {
				System.err.printf(
					"view :: Not a valid sequence name or index: '%s'\n", refStr);
				errors = true;
				continue;
			}

			final SAMFileSpan span = index.getSpanOverlapping(ref, beg, end);
			if (span == null)
				continue;

			try {
				final SAMRecordIterator it = reader.iterator(span);

				// This containment checking seems like something that should be
				// handled by the SAMFileSpan, but no such luck...
				//
				// Because they're in order, we can get by without doing the full
				// two-comparison containment check on each record: loop until a
				// record in range is found and then loop until one out of range is
				// found.

				while (it.hasNext()) {
					final SAMRecord rec = it.next();
					if (rec.getAlignmentEnd() >= beg) {
						if (rec.getAlignmentStart() <= end)
							writer.writeAlignment(rec);
						break;
					}
				}
				while (it.hasNext()) {
					final SAMRecord rec = it.next();
					if (rec.getAlignmentStart() <= end)
						writer.writeAlignment(rec);
					else
						break;
				}
			} catch (SAMFormatException e) {
				writer.close();
				System.err.printf("view :: Could not parse '%s': %s\n",
				                  path, e.getMessage());
				return 4;
			}
		}
		writer.close();
		return errors ? 5 : 0;
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
