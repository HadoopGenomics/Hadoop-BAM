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

// File created: 2011-06-14 13:00:09

package fi.tkk.ics.hadoop.bam.cli;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.TreeMap;

import org.apache.hadoop.util.GenericOptionsParser;

public final class Frontend {
	public static final int
		VERSION_MAJOR = 3,
		VERSION_MINOR = 3;

	public static void main(String[] args) {
		final Map<String, CLIPlugin> plugins = new TreeMap<String, CLIPlugin>();

		final ServiceLoader<CLIPlugin> pluginLoader =
			ServiceLoader.load(CLIPlugin.class);

		for (final Iterator<CLIPlugin> it = pluginLoader.iterator();;) {
			final CLIPlugin p;
			try {
				if (!it.hasNext())
					break;
				p = it.next();

			} catch (ServiceConfigurationError e) {
				System.err.println(
					"Warning: ServiceConfigurationError while loading plugins:");
				e.printStackTrace();
				continue;
			}
			plugins.put(p.getCommandName(), p);
		}

		if (plugins.isEmpty()) {
			System.err.println(
				"Error: no CLI plugins found: no functionality available");
			System.exit(1);
		}

		GenericOptionsParser parser;
		try {
			parser = new GenericOptionsParser(args);

		// This should be IOException but Hadoop 0.20.2 doesn't throw it...
		} catch (Exception e) {
			System.err.printf("Error in Hadoop arguments: %s\n", e.getMessage());
			System.exit(1);

			// Hooray for javac
			return;
		}
		args = parser.getRemainingArgs();

		Utils.setArgv0Class(Frontend.class);

		if (args.length == 0) {
			usage(System.err, plugins);
			System.exit(1);
		}

		final String command = args[0];
		if (command.equals("--help")) {
			usage(System.out, plugins);
			System.exit(0);
		}

		final CLIPlugin p = plugins.get(command);
		if (p == null) {
			System.err.printf("Unknown command '%s'\n", command);
			System.exit(1);
		}

		p.setConf(parser.getConfiguration());
		System.exit(p.main(Arrays.asList(args).subList(1, args.length)));
	}

	public static void usage(PrintStream out, Map<String, CLIPlugin> plugins) {
		out.printf("hadoop-bam version %d.%d command line frontend\n",
		           VERSION_MAJOR, VERSION_MINOR);
		out.printf("Usage: %s <command> [options]\n", Utils.getArgv0());

		int cmdLen = 0;
		for (final String cmd : plugins.keySet())
			cmdLen = Math.max(cmdLen, cmd.length());

		final String cmdFmt = "%-" + cmdLen + "s  ";

		out.print("\nCommand: ");
		final int  cmdPos = "Command: ".length(),
		          descPos = cmdPos + cmdLen + 2;

		boolean first = true;
		for (Map.Entry<String, CLIPlugin> entry : plugins.entrySet()) {
			if (first)
				first = false;
			else
				for (int i = cmdPos; i-- > 0;)
					out.print(' ');

			out.printf(cmdFmt, entry.getKey());

			Utils.printWrapped(out, entry.getValue().getDescription(), descPos);
		}
	}
}
