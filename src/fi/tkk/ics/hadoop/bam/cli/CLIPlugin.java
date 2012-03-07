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

// File created: 2011-06-14 13:41:56

package fi.tkk.ics.hadoop.bam.cli;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configured;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;

import fi.tkk.ics.hadoop.bam.util.Pair;

public abstract class CLIPlugin extends Configured {
	protected final String command, desc, usageTail, version, longDesc;

	protected final List<Pair<CmdLineParser.Option, String>> optionDescs;

	protected CLIPlugin(
		String commandName, String description, String version,
		String usageParams,
		List<Pair<CmdLineParser.Option, String>> optionDescriptions,
		String longDescription)
	{
		this.command     = commandName;
		this.desc        = description;
		this.version     = version;
		this.usageTail   = usageParams;
		this.optionDescs = optionDescriptions;
		this.longDesc    = longDescription;
	}

	public final String getCommandName() { return command; }
	public final String getDescription() { return desc;    }

	public void printUsage(PrintStream out) {
		out.printf("hadoop-bam version %d.%d command line: %s version %s\n",
		           Frontend.VERSION_MAJOR, Frontend.VERSION_MINOR,
		           command, version);

		out.printf("Usage: %s %s ", Utils.getArgv0(), command);

		if (optionDescs != null && !optionDescs.isEmpty())
			out.print("[options] ");

		out.println(usageTail);

		if (!longDesc.isEmpty()) {
			out.print('\n');
			Utils.printWrapped(out, longDesc);
		}

		if (optionDescs == null || optionDescs.isEmpty())
			return;

		Collections.sort(optionDescs,
			new Comparator<Pair<CmdLineParser.Option, String>>() {
				public int compare(
					Pair<CmdLineParser.Option, String> ap,
					Pair<CmdLineParser.Option, String> bp)
				{
					// Sort lexicographically, preferring the short form if it is
					// available, with case-insensitivity when comparing short and
					// long forms, and preferring lower case to upper in short
					// forms.

					final CmdLineParser.Option a = ap.fst,
					                           b = bp.fst;

					final String as = a.shortForm(), al = a.longForm(),
					             bs = b.shortForm(), bl = b.longForm();

					if (as != null && bs != null) {
						assert as.length() == 1;
						assert bs.length() == 1;

						final char ac = as.charAt(0),
						           bc = bs.charAt(0);

						if (Character.toLowerCase(ac) == Character.toLowerCase(bc))
							return Character.isUpperCase(ac) ? 1 : -1;

						return as.compareTo(bs);
					}

					if (as == null) {
						if (bs != null)
							return al.compareToIgnoreCase(bs);
						return al.compareTo(bl);
					}

					assert as != null;
					assert bs == null;

					return as.compareToIgnoreCase(bl);
				}
			});

		int optLen = 0;
		boolean anyShortForms = false;
		for (final Pair<CmdLineParser.Option, String> pair : optionDescs) {
			final CmdLineParser.Option opt = pair.fst;
			if (opt.shortForm() != null)
				anyShortForms = true;

			optLen = Math.max(optLen, opt.longForm().length());
		}

		final String optFmt = "--%-" + optLen + "s  ";

		out.print("\nOptions: ");
		final int  optPos = "Options: ".length(),
		          descPos = optPos + 2+optLen + 2
		                  + (anyShortForms ? "-x, ".length() : 0);

		boolean first = true;
		for (final Pair<CmdLineParser.Option, String> pair : optionDescs) {
			if (first)
				first = false;
			else
				for (int i = optPos; i-- > 0;)
					out.print(' ');

			final CmdLineParser.Option opt = pair.fst;

			if (anyShortForms)  {
				if (opt.shortForm() == null)
					out.print("    ");
				else
					out.printf("-%s, ", opt.shortForm());
			}

			out.printf(optFmt, opt.longForm());

			Utils.printWrapped(out, pair.snd, descPos);
		}
	}

	// Parses the parameters, handling an empty list as well as --help, before
	// passing control to run().
	public final int main(List<String> params) {
		if (params.isEmpty()) {
			printUsage(System.err);
			return 2;
		}
		if (params.contains("--help")) {
			printUsage(System.out);
			return 0;
		}

		final CmdLineParser parser = new CmdLineParser();
		if (optionDescs != null) {
			for (final Pair<CmdLineParser.Option, String> pair : optionDescs) {
				final String lf = pair.fst.longForm();
				final int    eq = lf.lastIndexOf('=');
				pair.fst.setLongForm(eq == -1 ? lf : lf.substring(0, eq));
				parser.addOption(pair.fst);
			}
		}

		try {
			parser.parse(params);
		} catch (CmdLineParser.OptionException e) {
			System.err.printf("%s :: %s.\n", this.command, e.getMessage());
			return 2;
		}
		return run(parser);
	}

	protected abstract int run(CmdLineParser parser);
}
