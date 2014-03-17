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

// File created: 2011-07-18 10:10:45

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import net.sf.samtools.BAMIndexer;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFormatException;
import net.sf.samtools.SAMRecordIterator;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.cli.Utils;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

public final class Index extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		stringencyOpt = new StringOption("validation-stringency=S");

	public Index() {
		super("index", "BAM indexing", "1.1", "PATH [OUT]", optionDescs,
			"Indexes the BAM file in PATH to OUT, or PATH.bai by default.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			stringencyOpt, Utils.getStringencyOptHelp()));
	}

	@Override protected int run(CmdLineParser parser) {

		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("index :: PATH not given.");
			return 3;
		}
		if (args.size() > 2) {
			System.err.printf(
				"index :: Too many arguments: expected at most 2, got %d.\n",
				args.size());
			return 3;
		}

		final SAMFileReader.ValidationStringency stringency =
			Utils.toStringency(parser.getOptionValue(stringencyOpt, SAMFileReader.ValidationStringency.DEFAULT_STRINGENCY.toString()), "index");
		if (stringency == null)
			return 3;

		final String path = args.get(0);
		final String out  = args.size() > 1 ? args.get(1) : path + ".bai";

		final SAMFileReader reader;
		try {
			reader = new SAMFileReader(
				WrapSeekable.openPath(getConf(), new Path(path)), false);
		} catch (Exception e) {
			System.err.printf("index :: Could not open '%s': %s\n",
			                  path, e.getMessage());
			return 4;
		}

		final SAMFileHeader header;
		try {
			header = reader.getFileHeader();
		} catch (SAMFormatException e) {
			System.err.printf("index :: Could not parse '%s': %s\n",
			                  path, e.getMessage());
			return 6;
		}

		final BAMIndexer indexer;
		try {
			final Path p = new Path(out);
			indexer = new BAMIndexer(p.getFileSystem(getConf()).create(p),
			                         header);
		} catch (Exception e) {
			System.err.printf("index :: Could not open '%s' for output: %s\n",
			                  out, e.getMessage());
			return 5;
		}

		// Necessary lest the BAMIndexer complain
		reader.enableFileSource(true);

		final SAMRecordIterator it = reader.iterator();
		try {
			while (it.hasNext())
				indexer.processAlignment(it.next());
		} catch (SAMFormatException e) {
			System.err.printf("index :: Could not parse '%s': %s\n",
			                  path, e.getMessage());
			return 6;
		}
		indexer.finish();
		return 0;
	}
}
