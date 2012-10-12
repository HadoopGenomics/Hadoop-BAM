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

// File created: 2011-06-14 14:52:20

package fi.tkk.ics.hadoop.bam.cli;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public final class Utils {
	public static void printWrapped(PrintStream out, String str) {
		printWrapped(out, str, 0);
	}
	public static void printWrapped(PrintStream out, String str, int indent) {
		printWrapped(out, str, indent, 80);
	}
	public static void printWrapped(
		PrintStream out, String str, int indent, int wrapAt)
	{
		int pos = indent;

		final Scanner words = new Scanner(str);

		// Print the first word out here to avoid a spurious line break if it
		// would wrap.
		if (words.hasNext()) {
			final String word = words.next();
			out.print(word);
			pos += word.length();
		}
		boolean addSpace = true;
		while (words.hasNext()) {
			final String word = words.next();
			final int    wend = words.match().end();

			pos += word.length();
			if (addSpace)
				++pos;

			if (pos < wrapAt) {
				if (addSpace)
					out.print(' ');
				out.print(word);
			} else {
				pos = indent + word.length();
				out.print('\n');
				for (int i = indent; i-- > 0;)
					out.print(' ');
				out.print(word);
			}

			if (wend < str.length() && str.charAt(wend) == '\n') {
				pos = indent;
				addSpace = false;

				int i = wend;
				do
					out.print('\n');
				while (++i < str.length() && str.charAt(i) == '\n');

				for (i = indent; i-- > 0;)
					out.print(' ');
			} else
				addSpace = true;
		}
		out.print('\n');
	}

	private static String   argv0 = null;
	private static Class<?> argv0Class = null;

	// For printing something intelligent in "Usage: argv0 <args>" messages.
	public static String getArgv0() {
		if (argv0 != null)
			return argv0;
		if (argv0Class == null)
			return null;

		final CodeSource cs =
			argv0Class.getProtectionDomain().getCodeSource();
		if (cs == null)
			return null;

		final String path = cs.getLocation().getPath();
		if (path.endsWith("/")) {
			// Typically (always?) a .class file loaded from the directory in
			// path.
			argv0 = argv0Class.getSimpleName();
		} else {
			// Typically (always?) a .jar file.
			argv0 = new File(path).getName();
		}

		return argv0;
	}
	public static void setArgv0Class(Class<?> cl) {
		argv0Class = cl;
		argv0 = null;
	}

	public static void configureSampling(
			Path workDir, String outName, Configuration conf)
		throws IOException
	{
		final Path partition =
			workDir.getFileSystem(conf).makeQualified(
				new Path(workDir, "_partitioning" + outName));

		TotalOrderPartitioner.setPartitionFile(conf, partition);
		try {
			final URI partitionURI = new URI(
				partition.toString() + "#" + partition.getName());

			if (partitionURI.getScheme().equals("file"))
				return;

			DistributedCache.addCacheFile(partitionURI, conf);
			DistributedCache.createSymlink(conf);
		} catch (URISyntaxException e) { throw new RuntimeException(e); }
	}
}
