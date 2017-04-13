// Copyright (c) 2012 Aalto University
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

// File created: 2012-02-22 20:40:39

package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for SAM, BAM, and CRAM files.
 * Values are the individual records; see {@link BAMRecordReader} for the
 * meaning of the key.
 *
 * <p>By default, files are recognized as SAM, BAM, or CRAM based on their file
 * extensions: see {@link #TRUST_EXTS_PROPERTY}. If that fails, or this
 * behaviour is disabled, the first byte of each file is read to determine the
 * file type.</p>
 */
public class AnySAMInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	/** A Boolean property: are file extensions trusted? The default is
	 * <code>true</code>.
	 *
	 * @see SAMFormat#inferFromFilePath
	 */
	public static final String TRUST_EXTS_PROPERTY =
		"hadoopbam.anysam.trust-exts";

	private final BAMInputFormat bamIF = new BAMInputFormat();
	private final CRAMInputFormat cramIF = new CRAMInputFormat();
	private final SAMInputFormat samIF = new SAMInputFormat();

	private final Map<Path,SAMFormat> formatMap;
	private final boolean              givenMap;

	private Configuration conf;

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
	 * <code>AnySAMInputFormat</code> can be used directly in
	 * <code>Job.setInputFormatClass</code>.
	 */
	public AnySAMInputFormat() {
		this(null, new HashMap<>(), false);
	}

	/** Creates a new input format, reading {@link #TRUST_EXTS_PROPERTY} from
	 * the given <code>Configuration</code>.
	 */
	public AnySAMInputFormat(Configuration conf) {
		this(conf, new HashMap<>(), false);
	}

	private static boolean trustExtensions(Configuration conf) {
		return conf.getBoolean(TRUST_EXTS_PROPERTY, true);
	}

	/** Creates a new input format, trusting the given <code>Map</code> to
	 * define the file-to-format associations. Neither file paths nor their
	 * contents are looked at, only the <code>Map</code> is used.
	 *
	 * <p>The <code>Map</code> is not copied, so it should not be modified while
	 * this input format is in use!</p>
	 * */
	public AnySAMInputFormat(Map<Path,SAMFormat> formatMap) {
		this(null, formatMap, true);
	}

	private AnySAMInputFormat(Configuration conf, Map<Path, SAMFormat> formatMap, boolean givenMap){
		this.formatMap = formatMap;
		this.givenMap = givenMap;
		this.conf = conf;
	}

	/** Returns the {@link SAMFormat} corresponding to the given path. Returns
	 * <code>null</code> if it cannot be determined even based on the file
	 * contents (unless future SAM/BAM formats are very different, this means
	 * that the path does not refer to a SAM or BAM file).
	 *
	 * <p>If this input format was constructed using a given
	 * <code>Map&lt;Path,SAMFormat&gt;</code> and the path is not contained
	 * within that map, throws an {@link IllegalArgumentException}.</p>
	 */
	public SAMFormat getFormat(final Path path) throws PathNotFoundException {
		SAMFormat fmt = formatMap.get(path);
		if (fmt != null || formatMap.containsKey(path))
			return fmt;

		if (givenMap)
			throw new IllegalArgumentException(
				"SAM format for '"+path+"' not in given map");

		if (this.conf == null)
			throw new IllegalStateException("Don't have a Configuration yet");

		if (trustExtensions(conf)) {
			final SAMFormat f = SAMFormat.inferFromFilePath(path);
			if (f != null) {
				formatMap.put(path, f);
				return f;
			}
		}

		try {
			FileSystem fileSystem = path.getFileSystem(conf);
			if (!fileSystem.exists(path)) {
				throw new PathNotFoundException(path.toString());
			}
			fmt = SAMFormat.inferFromData(fileSystem.open(path));
		} catch (IOException e) {}

		formatMap.put(path, fmt);
		return fmt;
	}

	/** Returns a {@link BAMRecordReader} or {@link SAMRecordReader} as
	 * appropriate, initialized with the given parameters.
	 *
	 * <p>Throws {@link IllegalArgumentException} if the given input split is
	 * not a {@link FileVirtualSplit} (used by {@link BAMInputFormat}) or a
	 * {@link FileSplit} (used by {@link SAMInputFormat}), or if the path
	 * referred to is not recognized as a SAM, BAM, or CRAM file (see {@link
	 * #getFormat}).</p>
	 */
	@Override public RecordReader<LongWritable,SAMRecordWritable>
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
			this.conf = ctx.getConfiguration();

		final SAMFormat fmt = getFormat(path);
		if (fmt == null)
			throw new IllegalArgumentException(
				"unknown SAM format, cannot create RecordReader: "+path);

		switch (fmt) {
			case SAM: return samIF.createRecordReader(split, ctx);
			case BAM: return bamIF.createRecordReader(split, ctx);
			case CRAM: return cramIF.createRecordReader(split, ctx);
			default: assert false; return null;
		}
	}

	/** Defers to {@link BAMInputFormat}, {@link CRAMInputFormat}, or
	 * {@link SAMInputFormat} as appropriate for the given path.
	 */
	@Override public boolean isSplitable(JobContext job, Path path) {
		if (this.conf == null)
			this.conf = job.getConfiguration();

		try {
			final SAMFormat fmt = getFormat(path);
			if (fmt == null)
        return super.isSplitable(job, path);

			switch (fmt) {
        case SAM: return samIF.isSplitable(job, path);
        case BAM: return bamIF.isSplitable(job, path);
        case CRAM: return cramIF.isSplitable(job, path);
        default: assert false; return false;
      }
		} catch (PathNotFoundException e) {
			return super.isSplitable(job, path);
		}
	}

	/** Defers to {@link BAMInputFormat} or {@link CRAMInputFormat} as appropriate for each
	 * individual path. SAM paths do not require special handling, so their splits are left
	 * unchanged.
	 */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		if (this.conf == null)
			this.conf = job.getConfiguration();

		final List<InputSplit> origSplits =
				BAMInputFormat.removeIndexFiles(super.getSplits(job));

		// We have to partition the splits by input format and hand them over to
		// the *InputFormats for any further handling.
		//
		// BAMInputFormat and CRAMInputFormat need to change the split boundaries, so we can
		// just extract the BAM and CRAM ones and leave the rest as they are.

		final List<InputSplit>
			bamOrigSplits = new ArrayList<InputSplit>(origSplits.size()),
			cramOrigSplits = new ArrayList<InputSplit>(origSplits.size()),
			newSplits     = new ArrayList<InputSplit>(origSplits.size());

		for (final InputSplit iSplit : origSplits) {
			final FileSplit split = (FileSplit)iSplit;

			if (SAMFormat.BAM.equals(getFormat(split.getPath())))
				bamOrigSplits.add(split);
			else if (SAMFormat.CRAM.equals(getFormat(split.getPath())))
				cramOrigSplits.add(split);
			else
				newSplits.add(split);
		}
		newSplits.addAll(bamIF.getSplits(bamOrigSplits, job.getConfiguration()));
		newSplits.addAll(cramIF.getSplits(cramOrigSplits, job.getConfiguration()));
		return newSplits;
	}
}
