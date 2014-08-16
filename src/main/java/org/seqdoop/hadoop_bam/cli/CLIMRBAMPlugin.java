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

// File created: 2013-06-20 14:22:25

package org.seqdoop.hadoop_bam.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;

import org.seqdoop.hadoop_bam.custom.jargs.gnu.CmdLineParser;
import static org.seqdoop.hadoop_bam.custom.jargs.gnu.CmdLineParser.Option.*;

import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.AnySAMOutputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringAnySAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.cli.CLIMRPlugin;
import org.seqdoop.hadoop_bam.util.Pair;

/** Like CLIMRPlugin, with added useful defaults for plugins working solely on
 * BAM/SAM files.
 *
 * Adds an option description for the "-o" option in CLIMRPlugin, so subclasses
 * shouldn't.
 */
public abstract class CLIMRBAMPlugin extends CLIMRPlugin {
	protected SAMFormat samFormat = null;

	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	protected static final CmdLineParser.Option
		formatOpt      = new  StringOption('F', "format=FMT"),
		noTrustExtsOpt = new BooleanOption("no-trust-exts");

	protected CLIMRBAMPlugin(
		String commandName, String description, String version,
		String usageParams, List<Pair<CmdLineParser.Option, String>> optionDescs,
		String longDescription)
	{
		// "call to super must be first statement in constructor" so you get that
		// lovely expression instead of two simple statements.
		super(
			commandName, description, version, usageParams,
			optionDescs == null
				? CLIMRBAMPlugin.optionDescs
				: (optionDescs.addAll(CLIMRBAMPlugin.optionDescs)
				   	? optionDescs : optionDescs),
			longDescription);
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			noTrustExtsOpt, "detect SAM/BAM files only by contents, "+
			                "never by file extension"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			formatOpt, "select the output format based on FMT: SAM or BAM"));

		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			outputPathOpt, "output a complete SAM/BAM file to the file PATH, "+
			               "removing the parts from WORKDIR; SAM/BAM is chosen "+
			               "by file extension, if appropriate (but -F takes "+
			               "precedence)"));
	}

	/** Should be called before accessing any of the protected data such as
	 * samFormat.
	 */
	@Override public boolean cacheAndSetProperties(CmdLineParser parser) {
		if (!super.cacheAndSetProperties(parser))
			return false;

		if (!cacheSAMFormat(parser))
			return false;

		final Configuration conf = getConf();

		conf.setBoolean(AnySAMInputFormat.TRUST_EXTS_PROPERTY,
		                !parser.getBoolean(noTrustExtsOpt));

		// Let the output format know if we're going to merge the output, so that
		// it doesn't write headers into the intermediate files.
		conf.setBoolean(KeyIgnoringAnySAMOutputFormat.WRITE_HEADER_PROPERTY,
		                outPath == null);

		return true;
	}

	private boolean cacheSAMFormat(CmdLineParser parser) {
		final String f = (String)parser.getOptionValue(formatOpt);
		if (f != null) {
			try { samFormat = SAMFormat.valueOf(f.toUpperCase(Locale.ENGLISH)); }
			catch (IllegalArgumentException e) {
				System.err.printf("%s :: invalid format '%s'\n",
				                  getCommandName(), f);
				return false;
			}
		}
		if (samFormat == null)
			samFormat = outPath == null ? SAMFormat.BAM
			                            : SAMFormat.inferFromFilePath(outPath);

		getConf().set(AnySAMOutputFormat.OUTPUT_SAM_FORMAT_PROPERTY,
		              samFormat.toString());
		return true;
	}
}
