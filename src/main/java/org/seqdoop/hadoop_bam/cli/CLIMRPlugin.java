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

// File created: 2013-06-20 14:17:25

package org.seqdoop.hadoop_bam.cli;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import org.seqdoop.hadoop_bam.custom.jargs.gnu.CmdLineParser;
import static org.seqdoop.hadoop_bam.custom.jargs.gnu.CmdLineParser.Option.*;

import org.seqdoop.hadoop_bam.cli.CLIPlugin;
import org.seqdoop.hadoop_bam.util.Pair;

/** Like CLIPlugin, but has lots of useful defaults for MapReduce-using
 * plugins.
 */
public abstract class CLIMRPlugin extends CLIPlugin {
	protected boolean verbose;
	protected int     reduceTasks;
	protected Path    outPath;

	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	protected static final CmdLineParser.Option
		reducersOpt = new IntegerOption('r', "reducers=N"),
		verboseOpt  = new BooleanOption('v', "verbose"),

		/** Left undescribed here because there may be relevant
		 * application-specific details.
		 */
		outputPathOpt = new StringOption('o', "output-merged-path=PATH");

	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			reducersOpt, "use N reduce tasks (default: 1), i.e. produce N "+
			              "outputs in parallel"));
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			verboseOpt, "tell the Hadoop job to be more verbose"));
	}

	protected CLIMRPlugin(
		String commandName, String description, String version,
		String usageParams, List<Pair<CmdLineParser.Option, String>> optionDescs,
		String longDescription)
	{
		// "call to super must be first statement in constructor" so you get that
		// lovely expression instead of two simple statements.
		super(
			commandName, description, version, usageParams,
			optionDescs == null
				? CLIMRPlugin.optionDescs
				: (optionDescs.addAll(CLIMRPlugin.optionDescs)
				   	? optionDescs : optionDescs),
			longDescription);
	}

	/** Should be called before accessing any of the protected data such as
	 * verbose.
	 */
	public boolean cacheAndSetProperties(CmdLineParser parser) {
		verbose     = parser.getBoolean(verboseOpt);
		reduceTasks = parser.getInt    (reducersOpt, 1);

		final String out = (String)parser.getOptionValue(outputPathOpt);
		outPath = out == null ? null : new Path(out);

		getConf().setInt("mapred.reduce.tasks",   reduceTasks);
		getConf().setInt("mapreduce.job.reduces", reduceTasks);

		return true;
	}
}
