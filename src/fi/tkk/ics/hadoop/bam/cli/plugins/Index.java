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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import net.sf.samtools.SAMFormatException;
import net.sf.samtools.SAMFileReader.ValidationStringency;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import fi.tkk.ics.hadoop.bam.custom.samtools.BAMIndexer;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileHeader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecordIterator;

import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

public final class Index extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		localInputOpt  = new BooleanOption('L', "--local-input"),
		localOutputOpt = new BooleanOption('O', "--local-output");

	public Index() {
		super("index", "BAM indexing", "1.0", "PATH [OUT]", optionDescs,
			"Indexes the BAM file in PATH to OUT, or PATH.bai by default."+
			"\n\n"+
			"By default, PATH is treated as a local file path if run outside "+
			"Hadoop and an HDFS path if run within it.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			localInputOpt, "treat PATH to referring to the local FS, not HDFS"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			localOutputOpt, "treat OUT as referring to the local FS, not HDFS"));
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

		final String path = args.get(0);
		final String out  = args.size() > 1 ? args.get(1) : null;

		final boolean
			localInput  = parser.getBoolean(localInputOpt),
			localOutput = parser.getBoolean(localOutputOpt);

		final SAMFileReader reader;

		try {
			if (localInput)
				reader = new SAMFileReader(new File(path), false);
			else {
				final Path p = new Path(path);
				reader = new SAMFileReader(WrapSeekable.openPath(getConf(), p),
				                           false);
			}
		} catch (Exception e) {
			System.err.printf("index :: Could not open '%s': %s\n",
			                  path, e.getMessage());
			return 4;
		}

		reader.setValidationStringency(ValidationStringency.SILENT);

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
			if (localOutput)
				indexer = new BAMIndexer(new File(out), header);
			else {
				final Path p = new Path(out);
				indexer = new BAMIndexer(p.getFileSystem(getConf()).create(p),
				                         header);
			}
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
