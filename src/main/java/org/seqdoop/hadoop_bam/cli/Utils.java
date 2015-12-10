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

package org.seqdoop.hadoop_bam.cli;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import htsjdk.samtools.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import htsjdk.samtools.util.BlockCompressedStreamConstants;

import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.util.SAMOutputPreparer;
import org.seqdoop.hadoop_bam.util.Timer;

import hbparquet.hadoop.util.ContextUtil;

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

	public static final String WORK_FILENAME_PROPERTY =
		"hadoopbam.work.filename";

	/** Returns a name that mergeInto() will recognize as a file to be merged.
	 *
	 * The filename is the value of WORK_FILENAME_PROPERTY surrounded by
	 * basePrefix and basePostfix, followed by the part number and with the
	 * given extension.
	 */
	public static Path getMergeableWorkFile(
			Path directory, String basePrefix, String basePostfix,
			TaskAttemptContext ctx, String extension)
	{
		return new Path(
			directory,
			  basePrefix
			+ ContextUtil.getConfiguration(ctx).get(WORK_FILENAME_PROPERTY)
			+ basePostfix
			+ "-"
			+ String.format("%06d", ctx.getTaskAttemptID().getTaskID().getId())
			+ (extension.isEmpty() ? extension : "." + extension));
	}

	/** Merges the files in the given directory that have names given by
	 * getMergeableWorkFile() into out.
	 *
	 * Outputs progress reports if commandName is non-null.
	 */
	public static void mergeInto(
			OutputStream out,
			Path directory, String basePrefix, String basePostfix,
			Configuration conf, String commandName)
		throws IOException
	{
		final FileSystem fs = directory.getFileSystem(conf);

		final FileStatus[] parts = fs.globStatus(new Path(
			directory,
			  basePrefix + conf.get(WORK_FILENAME_PROPERTY) + basePostfix
			+ "-[0-9][0-9][0-9][0-9][0-9][0-9]*"));

		int i = 0;
		Timer t = new Timer();
		for (final FileStatus part : parts) {
			if (commandName != null) {
				System.out.printf("%s :: Merging part %d (size %d)...",
				                  commandName, ++i, part.getLen());
				System.out.flush();

				t.start();
			}

			final InputStream in = fs.open(part.getPath());
			IOUtils.copyBytes(in, out, conf, false);
			in.close();

			if (commandName != null)
				System.out.printf(" done in %d.%03d s.\n", t.stopS(), t.fms());
		}
		for (final FileStatus part : parts)
			fs.delete(part.getPath(), false);
	}

	private static final String
		HEADERMERGER_SORTORDER_PROP = "hadoopbam.headermerger.sortorder";

	public static void setHeaderMergerSortOrder(
		Configuration conf, SAMFileHeader.SortOrder order)
	{
		conf.set(HEADERMERGER_SORTORDER_PROP, order.name());
	}

	public static final String
		HEADERMERGER_INPUTS_PROPERTY = "hadoopbam.headermerger.inputs";

	private static SamFileHeaderMerger headerMerger = null;

	/** Computes the merger of the SAM headers in the files listed in
	 * HEADERMERGER_INPUTS_PROPERTY. The sort order of the result is set
	 * according to the last call to setHeaderMergerSortOrder, or otherwise
	 * to "unsorted".
	 *
	 * The result is cached locally to prevent it from being recomputed too
	 * often.
	 */
	public static SamFileHeaderMerger getSAMHeaderMerger(Configuration conf)
		throws IOException
	{
		// TODO: it would be preferable to cache this beforehand instead of
		// having every task read the header block of every input file. But that
		// would be trickier, given that SamFileHeaderMerger isn't trivially
		// serializable.

		// Save it in a static field, though, in case that helps anything.
		if (headerMerger != null)
			return headerMerger;

		final List<SAMFileHeader> headers = new ArrayList<SAMFileHeader>();

		for (final String in : conf.getStrings(HEADERMERGER_INPUTS_PROPERTY)) {
			final Path p = new Path(in);

			final SamReader r = SamReaderFactory.makeDefault().open(
					SamInputResource.of(p.getFileSystem(conf).open(p)));
			headers.add(r.getFileHeader());
			r.close();
		}

		final String orderStr = conf.get(HEADERMERGER_SORTORDER_PROP);
		final SAMFileHeader.SortOrder order =
			orderStr == null ? SAMFileHeader.SortOrder.unsorted
			                 : SAMFileHeader.SortOrder.valueOf(orderStr);

		return headerMerger = new SamFileHeaderMerger(order, headers, true);
	}

	/** Changes the given SAMRecord as appropriate for being placed in a file
	 * whose header is getSAMHeaderMerger(conf).getMergedHeader().
	 */
	public static void correctSAMRecordForMerging(
			SAMRecord r, Configuration conf)
		throws IOException
	{
		if (headerMerger == null)
			getSAMHeaderMerger(conf);

		final SAMFileHeader h = r.getHeader();

		// Correct the reference indices, and thus the key, if necessary.
		if (headerMerger.hasMergedSequenceDictionary()) {
			final int ri = headerMerger.getMergedSequenceIndex(
				h, r.getReferenceIndex());

			r.setReferenceIndex(ri);
			if (r.getReadPairedFlag())
				r.setMateReferenceIndex(headerMerger.getMergedSequenceIndex(
					h, r.getMateReferenceIndex()));
		}

		// Correct the program group if necessary.
		if (headerMerger.hasProgramGroupCollisions()) {
			final String pg = (String)r.getAttribute(
				ReservedTagConstants.PROGRAM_GROUP_ID);
			if (pg != null)
				r.setAttribute(
					ReservedTagConstants.PROGRAM_GROUP_ID,
					headerMerger.getProgramGroupId(h, pg));
		}

		// Correct the read group if necessary.
		if (headerMerger.hasReadGroupCollisions()) {
			final String rg = (String)r.getAttribute(
				ReservedTagConstants.READ_GROUP_ID);
			if (rg != null)
				r.setAttribute(
					ReservedTagConstants.READ_GROUP_ID,
					headerMerger.getProgramGroupId(h, rg));
		}
	}

	/** Merges the files in the given directory that have names given by
	 * getMergeableWorkFile() into out in the given SAMFormat, using
	 * getSAMHeaderMerger().getMergedHeader() as the header.
	 *
	 * Outputs progress reports if commandName is non-null.
	 */
	public static void mergeSAMInto(
			Path out, Path directory, String basePrefix, String basePostfix,
			SAMFormat format, Configuration conf, String commandName)
		throws IOException
	{
		Timer t = new Timer();
		System.out.printf("%s :: Merging output...\n", commandName);
		t.start();

		final OutputStream outs = out.getFileSystem(conf).create(out);

		// First, place the SAM or BAM header.
		//
		// Don't use the returned stream, because we're concatenating directly
		// and don't want to apply another layer of compression to BAM.
		new SAMOutputPreparer().prepareForRecords(
				outs, format, getSAMHeaderMerger(conf).getMergedHeader());

		// Then, the actual SAM or BAM contents.
		mergeInto(outs, directory, basePrefix, basePostfix, conf, commandName);

		// And if BAM, the BGZF terminator.
		if (format == SAMFormat.BAM)
			outs.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);

		outs.close();

		System.out.printf("%s :: Merging complete in %d.%03d s.\n",
		                  commandName, t.stopS(), t.fms());
	}

	private static String stringencyOptHelp = null;
	public static String getStringencyOptHelp() {
		if (stringencyOptHelp != null)
			return stringencyOptHelp;

		final StringBuilder sb = new StringBuilder(
			"set Picard's validation stringency to S (");

		String last = null;
		for (final ValidationStringency v
		     : ValidationStringency.values())
		{
			if (last != null) {
				sb.append(last);
				sb.append(", ");
			}
			last = v.name();
		}
		sb.append("or ");
		sb.append(last);
		sb.append(')');
		return stringencyOptHelp = sb.toString();
	}

	/** Sets the default validation stringency in addition to returning it. */
	@SuppressWarnings("deprecation")
	public static ValidationStringency toStringency(
		Object o, String cmdName)
	{
		final String s = (String)o;
		try {
			final ValidationStringency stringency =
				ValidationStringency.valueOf(s);
			SAMFileReader.setDefaultValidationStringency(stringency);
			return stringency;
		} catch (IllegalArgumentException e) {
			System.err.printf("%s :: invalid stringency '%s'\n", cmdName, s);
			return null;
		}
	}
}
