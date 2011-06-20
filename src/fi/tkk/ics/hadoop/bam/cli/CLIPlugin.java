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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public abstract class CLIPlugin {
	protected final String command, desc, usageTail;

	protected final NavigableMap<String, String> paramDescs;

	protected CLIPlugin(
		String commandName, String description, String usageParams,
		NavigableMap<String, String> paramDescriptions)
	{
		command    = commandName;
		desc       = description;
		usageTail  = usageParams;
		paramDescs = paramDescriptions;
	}

	public final String getCommandName() { return command; }
	public final String getDescription() { return desc;    }

	public void printUsage(PrintStream out) {
		out.printf("Usage: Frontend %s ", command);

		if (!paramDescs.isEmpty())
			out.print("[options] ");

		out.println(usageTail);

		if (paramDescs.isEmpty())
			return;

		int optLen = 0;
		for (final String opt : paramDescs.keySet())
			optLen = Math.max(optLen, opt.length());

		final String optFmt = "%-" + optLen + "s  ";

		out.print("\nOptions: ");
		final int descPos = "Options: ".length() + 2;

		boolean first = true;
		for (Map.Entry<String, String> entry : paramDescs.entrySet()) {
			if (first)
				first = false;
			else
				for (int i = descPos; i-- > 0;)
					out.print(' ');

			out.printf(optFmt, entry.getKey());

			Utils.printWrapped(out, entry.getValue(), descPos);
		}
	}

	// Handles a blank parameter list and --help and then passes control to
	// run().
	public final int main(List<String> params) {
		if (params.isEmpty()) {
			printUsage(System.err);
			return 2;
		}
		if (params.contains("--help")) {
			printUsage(System.out);
			return 0;
		}
		return run(params);
	}

	protected abstract int run(List<String> params);
}
