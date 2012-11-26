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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public final class Frontend {
	public static final int
		VERSION_MAJOR = 5,
		VERSION_MINOR = 1;

	public static void main(String[] args) {

		final Thread thread = Thread.currentThread();

		// This naming scheme should become clearer below.
		final URLClassLoader loader2 =
			(URLClassLoader)thread.getContextClassLoader();

		// Parse Hadoop's generic options first, so that -libjars is handled
		// before we try to load plugins.
		final GenericOptionsParser parser;
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
		final Configuration conf = parser.getConfiguration();

		final URLClassLoader
			loader1 = (URLClassLoader)thread.getContextClassLoader();

		if (loader1 != loader2) {
			/* Set the thread's context class loader to a new one that includes
			 * the URLs of both the current one and its parent. Replace those two
			 * completely: have the new one delegate to the current one's
			 * grandparent.
			 *
			 * This is necessary to support Hadoop's "-libjars" argument because
			 * of the way Hadoop's "hadoop jar" command works: it doesn't handle
			 * "-libjars" or anything like it, instead the GenericOptionsParser we
			 * use above does. Since URLs can't be added to class loaders,
			 * GenericOptionsParser creates a new loader, adds the paths given via
			 * "-libjars" to it, and makes it delegate to the loader created by
			 * "hadoop jar". So the class loader setup at this point looks like
			 * the following:
			 *
			 * 1. The loader that knows about the "-libjars" parameters.
			 * 2. The loader that knows about "hadoop jar"'s parameter: the jar we
			 *    are running.
			 * 3. The system class loader (I think), which was created when the
			 *    "hadoop" script ran "java"; it knows about the main Hadoop jars,
			 *    everything in HADOOP_CLASSPATH, etc.
			 *
			 * Here 3 is 2's parent and 2 is 1's parent. The result is that when
			 * loading our own plugins, we end up finding them in 2, of course.
			 * But if Picard was given in "-libjars", 2 can't see the dependencies
			 * of those plugins, because they're not visible to it or its parents,
			 * and thus throws a NoClassDefFoundError.
			 *
			 * Thus, we create a new class loader which combines 1 and 2 and
			 * delegates to 3.
			 *
			 * Only done inside this if statement because otherwise we didn't get
			 * "-libjars" and so loader 1 is missing, and we don't want to mess
			 * with loader 3.
			 */

			final URL[] urls1   = loader1.getURLs(),
			            urls2   = loader2.getURLs(),
			            allURLs = new URL[urls1.length + urls2.length];

			System.arraycopy(urls1, 0, allURLs,            0, urls1.length);
			System.arraycopy(urls2, 0, allURLs, urls1.length, urls2.length);

			thread.setContextClassLoader(
				new URLClassLoader(allURLs, loader2.getParent()));

			// Evidently we don't need to do conf.setClassLoader(). Presumably
			// Hadoop loads the libjars and the main jar in the same class loader.
		}

		/* Call the go(args,conf) method of this class, but do it via
		 * reflection, loading this class from the context class loader.
		 *
		 * This is because in Java, class identity is based on both class name
		 * and class loader. Using the same loader identifiers as in the
		 * previous comment, this class and the rest of Hadoop-BAM was
		 * originally loaded by loader 2. Therefore if we were to call
		 * go(args,conf) directly, plain old "CLIPlugin.class" would not be
		 * compatible with any plugins that the new class loader finds,
		 * because their CLIPlugin is a different CLIPlugin!
		 *
		 * Hence, jump into the new class loader. Both String[] and
		 * Configuration are from loader 1 and thus we can safely pass those
		 * from here (loader 2) to there (the new loader).
		 */
		try {
			final Class<?> frontendClass =
				Class.forName(Frontend.class.getName(), true,
				              thread.getContextClassLoader());

			final Method meth = frontendClass.getMethod(
				"go", args.getClass(), conf.getClass());

			meth.invoke(null, args, conf);

		} catch (InvocationTargetException e) {
			// Presumably some RuntimeException thrown by go() for some reason.
			e.getCause().printStackTrace();
			System.exit(1);

		} catch (ClassNotFoundException e) {
			System.err.println("VERY STRANGE: could not reload Frontend class:");
			e.printStackTrace();

		} catch (NoSuchMethodException e) {
			System.err.println("VERY STRANGE: could not find our own method:");
			e.printStackTrace();

		} catch (IllegalAccessException e) {
			System.err.println(
				"VERY STRANGE: not allowed to access our own method:");
			e.printStackTrace();
		}
		System.exit(112);
	}

	public static void go(String[] args, Configuration conf) {
		final Map<String, CLIPlugin> plugins = new TreeMap<String, CLIPlugin>();

		final ServiceLoader<CLIPlugin> pluginLoader =
			ServiceLoader.load(CLIPlugin.class);

		final Set<String> seenServiceConfigErrors = new HashSet<String>(0);

		for (final Iterator<CLIPlugin> it = pluginLoader.iterator();;) {
			final CLIPlugin p;
			try {
				if (!it.hasNext())
					break;
				p = it.next();

			} catch (ServiceConfigurationError e) {
				if (seenServiceConfigErrors.isEmpty())
					System.err.println("Warning: ServiceConfigurationErrors while "+
					                   "loading plugins:");

				final String msg = e.getMessage();
				if (!seenServiceConfigErrors.contains(msg)) {
					System.err.println(msg);
					seenServiceConfigErrors.add(msg);
				}
				continue;
			}
			plugins.put(p.getCommandName(), p);
		}

		if (plugins.isEmpty()) {
			System.err.println(
				"Error: no CLI plugins found: no functionality available");
			System.exit(1);
		}

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
			System.err.printf(
				"Unknown command '%s', see '--help' for help.\n", command);
			System.exit(1);
		}

		p.setConf(conf);
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
