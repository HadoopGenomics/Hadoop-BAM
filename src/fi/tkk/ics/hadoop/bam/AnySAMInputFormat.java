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

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;

public class AnySAMInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	public static final String TRUST_EXTS_PROPERTY =
		"hadoopbam.anysam.trust-exts";

	private final BAMInputFormat bamIF = new BAMInputFormat();
	private final SAMInputFormat samIF = new SAMInputFormat();

	private final Map<Path,SAMFormat> formatMap;

	private final Configuration conf;
	private final boolean trustExts,
	                      givenMap;

	public AnySAMInputFormat(Configuration conf) {
		this.formatMap = new HashMap<Path,SAMFormat>();
		this.conf      = conf;
		this.trustExts = conf.getBoolean(TRUST_EXTS_PROPERTY, true);
		this.givenMap  = false;
	}

	public AnySAMInputFormat(Map<Path,SAMFormat> formatMap) {
		this.formatMap = formatMap;
		this.givenMap  = true;

		// Arbitrary values.
		this.conf = null;
		this.trustExts = false;
	}

	public SAMFormat getFormat(final Path path) {
		SAMFormat fmt = formatMap.get(path);
		if (fmt != null || formatMap.containsKey(path))
			return fmt;

		if (givenMap)
			throw new IllegalArgumentException(
				"SAM format for '"+path+"' not in given map");

		if (trustExts) {
			final SAMFormat f = SAMFormat.inferFromFilePath(path);
			if (f != null) {
				formatMap.put(path, f);
				return f;
			}
		}

		try {
			final FSDataInputStream in = path.getFileSystem(conf).open(path);
			final byte b = in.readByte();
			in.close();

			switch (b) {
				case 0x1f: fmt = SAMFormat.BAM; break;
				case '@':  fmt = SAMFormat.SAM; break;
				default: break;
			}
		} catch (IOException e) {}

		formatMap.put(path, fmt);
		return fmt;
	}

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

		final SAMFormat fmt = getFormat(path);
		if (fmt == null)
			throw new IllegalArgumentException(
				"unknown SAM format, cannot create RecordReader: "+path);

		switch (fmt) {
			case SAM: return samIF.createRecordReader(split, ctx);
			case BAM: return bamIF.createRecordReader(split, ctx);
			default: assert false; return null;
		}
	}

	@Override public boolean isSplitable(JobContext job, Path path) {
		final SAMFormat fmt = getFormat(path);
		if (fmt == null)
			return super.isSplitable(job, path);

		switch (fmt) {
			case SAM: return samIF.isSplitable(job, path);
			case BAM: return bamIF.isSplitable(job, path);
			default: assert false; return false;
		}
	}

	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		final List<InputSplit> origSplits = super.getSplits(job);

		// We have to partition the splits by input format and hand them over to
		// the *InputFormats for any further handling.
		//
		// At least for now, BAMInputFormat is the only one that needs to mess
		// with them any further, so we can just extract the BAM ones and leave
		// the rest as they are.

		final List<InputSplit>
			bamOrigSplits = new ArrayList<InputSplit>(origSplits.size()),
			newSplits     = new ArrayList<InputSplit>(origSplits.size());

		for (final InputSplit iSplit : origSplits) {
			final FileSplit split = (FileSplit)iSplit;

			if (SAMFormat.BAM.equals(getFormat(split.getPath())))
				bamOrigSplits.add(split);
			else
				newSplits.add(split);
		}
		newSplits.addAll(bamIF.getSplits(bamOrigSplits, job.getConfiguration()));
		return newSplits;
	}
}
