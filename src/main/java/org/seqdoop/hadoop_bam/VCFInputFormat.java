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

// File created: 2013-06-26 12:47:26

package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import htsjdk.samtools.seekablestream.SeekableStream;

import org.seqdoop.hadoop_bam.util.WrapSeekable;

import hbparquet.hadoop.util.ContextUtil;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for VCF files. Values
 * are the individual records; see {@link VCFRecordReader} for the meaning of
 * the key.
 */
public class VCFInputFormat
	extends FileInputFormat<LongWritable,VariantContextWritable>
{
	/** Whether file extensions are to be trusted, defaults to true.
	 *
	 * @see VCFFormat#inferFromFilePath
	 */

	public static final String TRUST_EXTS_PROPERTY =
		"hadoopbam.vcf.trust-exts";

	private final Map<Path,VCFFormat> formatMap;
	private final boolean              givenMap;

	private Configuration conf;
	private boolean trustExts;

	/** Creates a new input format, which will use the
	 * <code>Configuration</code> from the first public method called. Thus this
	 * will behave as though constructed with a <code>Configuration</code>
	 * directly, but only after it has received it in
	 * <code>createRecordReader</code> (via the <code>TaskAttemptContext</code>)
	 * or <code>isSplitable</code> or <code>getSplits</code> (via the
	 * <code>JobContext</code>). Until then, other methods will throw an {@link
	 * IllegalStateException}.
	 *
	 * This constructor exists mainly as a convenience, e.g. so that
	 * <code>VCFInputFormat</code> can be used directly in
	 * <code>Job.setInputFormatClass</code>.
	 */
	public VCFInputFormat() {
		this.formatMap = new HashMap<Path,VCFFormat>();
		this.givenMap  = false;
		this.conf      = null;
	}

	/** Creates a new input format, reading {@link #TRUST_EXTS_PROPERTY} from
	 * the given <code>Configuration</code>.
	 */
	public VCFInputFormat(Configuration conf) {
		this.formatMap = new HashMap<Path,VCFFormat>();
		this.conf      = conf;
		this.trustExts = conf.getBoolean(TRUST_EXTS_PROPERTY, true);
		this.givenMap  = false;
	}

	/** Creates a new input format, trusting the given <code>Map</code> to
	 * define the file-to-format associations. Neither file paths nor their
	 * contents are looked at, only the <code>Map</code> is used.
	 *
	 * <p>The <code>Map</code> is not copied, so it should not be modified while
	 * this input format is in use!</p>
	 * */
	public VCFInputFormat(Map<Path,VCFFormat> formatMap) {
		this.formatMap = formatMap;
		this.givenMap  = true;

		// Arbitrary values.
		this.conf = null;
		this.trustExts = false;
	}

	/** Returns the {@link VCFFormat} corresponding to the given path. Returns
	 * <code>null</code> if it cannot be determined even based on the file
	 * contents (unless future VCF/BCF formats are very different, this means
	 * that the path does not refer to a VCF or BCF file).
	 *
	 * <p>If this input format was constructed using a given
	 * <code>Map&lt;Path,VCFFormat&gt;</code> and the path is not contained
	 * within that map, throws an {@link IllegalArgumentException}.</p>
	 */
	public VCFFormat getFormat(final Path path) {
		VCFFormat fmt = formatMap.get(path);
		if (fmt != null || formatMap.containsKey(path))
			return fmt;

		if (givenMap)
			throw new IllegalArgumentException(
				"VCF format for '"+path+"' not in given map");

		if (this.conf == null)
			throw new IllegalStateException("Don't have a Configuration yet");

		if (trustExts) {
			final VCFFormat f = VCFFormat.inferFromFilePath(path);
			if (f != null) {
				formatMap.put(path, f);
				return f;
			}
		}

		try {
			fmt = VCFFormat.inferFromData(path.getFileSystem(conf).open(path));
		} catch (IOException e) {}

		formatMap.put(path, fmt);
		return fmt;
	}

	/** Returns a {@link BCFRecordReader} or {@link VCFRecordReader} as
	 * appropriate, initialized with the given parameters.
	 *
	 * <p>Throws {@link IllegalArgumentException} if the given input split is
	 * not a {@link FileVirtualSplit} or a {@link FileSplit}, or if the path
	 * referred to is not recognized as a VCF or BCF file (see {@link
	 * #getFormat}).</p>
	 */
	@Override public RecordReader<LongWritable,VariantContextWritable>
		createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException
	{
		final Path path;
		if (split instanceof FileSplit)
			path = ((FileSplit)split).getPath();
		else if (split instanceof FileVirtualSplit)
			path = ((FileVirtualSplit)split).getPath();
		else
			throw new IllegalArgumentException(
				"split '"+split+"' has unknown type: cannot extract path");

		if (this.conf == null)
			this.conf = ContextUtil.getConfiguration(ctx);

		final VCFFormat fmt = getFormat(path);
		if (fmt == null)
			throw new IllegalArgumentException(
				"unknown VCF format, cannot create RecordReader: "+path);

		final RecordReader<LongWritable, VariantContextWritable> rr;

		switch (fmt) {
			case VCF: rr = new VCFRecordReader(); break;
			case BCF: rr = new BCFRecordReader(); break;
			default: assert false; return null;
		}

		rr.initialize(split, ctx);
		return rr;
	}

	/** Defers to {@link BCFSplitGuesser} as appropriate for each individual
	 * path. VCF paths do not require special handling, so their splits are left
	 * unchanged.
	 */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		if (this.conf == null)
			this.conf = ContextUtil.getConfiguration(job);

		final List<InputSplit> origSplits = super.getSplits(job);

		// We have to partition the splits by input format and hand the BCF ones
		// over to getBCFSplits().

		final List<FileSplit>
			bcfOrigSplits = new ArrayList<FileSplit>(origSplits.size());
		final List<InputSplit>
			newSplits     = new ArrayList<InputSplit>(origSplits.size());

		for (final InputSplit iSplit : origSplits) {
			final FileSplit split = (FileSplit)iSplit;

			if (VCFFormat.BCF.equals(getFormat(split.getPath())))
				bcfOrigSplits.add(split);
			else
				newSplits.add(split);
		}
		fixBCFSplits(bcfOrigSplits, newSplits);
		return newSplits;
	}

	// The given FileSplits should all be for BCF files. Adds InputSplits
	// aligned to record boundaries. Compressed BCF results in
	// FileVirtualSplits, uncompressed in FileSplits.
	private void fixBCFSplits(
			List<FileSplit> splits, List<InputSplit> newSplits)
		throws IOException
	{
		// addGuessedSplits() requires the given splits to be sorted by file
		// path, so do so. Although FileInputFormat.getSplits() does, at the time
		// of writing this, generate them in that order, we shouldn't rely on it.
		Collections.sort(splits, new Comparator<FileSplit>() {
			public int compare(FileSplit a, FileSplit b) {
				return a.getPath().compareTo(b.getPath());
			}
		});

		for (int i = 0; i < splits.size();)
			i = addGuessedSplits(splits, i, newSplits);
	}

	// Handles all the splits that share the Path of the one at index i,
	// returning the next index to be used.
	private int addGuessedSplits(
			List<FileSplit> splits, int i, List<InputSplit> newSplits)
		throws IOException
	{
		final Path path = splits.get(i).getPath();
		final SeekableStream sin = WrapSeekable.openPath(conf, path);

		final BCFSplitGuesser guesser = new BCFSplitGuesser(sin);

		final boolean isBGZF = guesser.isBGZF();

		InputSplit prevSplit = null;

		for (; i < splits.size(); ++i) {
			final FileSplit fspl = splits.get(i);
			if (!fspl.getPath().equals(path))
				break;

			final String[] locs = fspl.getLocations();

			final long beg =       fspl.getStart();
			final long end = beg + fspl.getLength();

			final long alignBeg = guesser.guessNextBCFRecordStart(beg, end);

			// As the guesser goes to the next BGZF block before looking for BCF
			// records, the ending BGZF blocks have to always be traversed fully.
			// Hence force the length to be 0xffff, the maximum possible.
			final long alignEnd = isBGZF ? end << 16 | 0xffff : end;

			final long length = alignEnd - alignBeg;

			if (alignBeg == end) {
				// No records detected in this split: merge it to the previous one.
				// This could legitimately happen e.g. if we have a split that is
				// so small that it only contains the middle part of a BGZF block.
				//
				// Of course, if it's the first split, then this is simply not a
				// valid BCF file.
				//
				// FIXME: In theory, any number of splits could only contain parts
				// of the BCF header before we start to see splits that contain BCF
				// records. For now, we require that the split size is at least as
				// big as the header and don't handle that case.
				if (prevSplit == null)
					throw new IOException("'" + path + "': no records in first "+
						"split: bad BCF file or tiny split size?");

				if (isBGZF) {
					((FileVirtualSplit)prevSplit).setEndVirtualOffset(alignEnd);
					continue;
				}
				prevSplit = new FileSplit(path, alignBeg, length, locs);
				newSplits.remove(newSplits.size() - 1);
			} else {
				prevSplit =
					isBGZF ? new FileVirtualSplit(path, alignBeg, alignEnd, locs)
					       : new FileSplit       (path, alignBeg, length,   locs);
			}
			newSplits.add(prevSplit);
		}

		sin.close();
		return i;
	}
}
