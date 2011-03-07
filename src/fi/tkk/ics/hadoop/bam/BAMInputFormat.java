// Copyright (c) 2010 Aalto University
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

// File created: 2010-08-03 11:50:19

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import net.sf.samtools.util.SeekableStream;

import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for BAM files. Values
 * are the individual records; see {@link BAMRecordReader} for the meaning of
 * the key.
 *
 * <p>A {@link SplittingBAMIndex} for each Path used is required, or an
 * <code>IOException</code> is thrown out of {@link #getSplits}.</p>
 */
public class BAMInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	private Path getIdxPath(Path path) { return path.suffix(".splitting-bai"); }

	/** Returns a {@link BAMRecordReader} initialized with the parameters. */
	@Override public RecordReader<LongWritable,SAMRecordWritable>
		createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException
	{
		final RecordReader<LongWritable,SAMRecordWritable> rr =
			new BAMRecordReader();
		rr.initialize(split, ctx);
		return rr;
	}

	/** The splits returned are FileVirtualSplits. */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		final List<InputSplit> splits = super.getSplits(job);

		// Align the splits so that they don't cross blocks.

		// addIndexedSplits() requires the given splits to be sorted by file
		// path, so do so. Although FileInputFormat.getSplits() does, at the time
		// of writing this, generate them in that order, we shouldn't rely on it.
		Collections.sort(splits, new Comparator<InputSplit>() {
			public int compare(InputSplit a, InputSplit b) {
				FileSplit fa = (FileSplit)a, fb = (FileSplit)b;
				return fa.getPath().compareTo(fb.getPath());
			}
		});

		final List<InputSplit> newSplits =
			new ArrayList<InputSplit>(splits.size());

		final Configuration cfg = job.getConfiguration();

		for (int i = 0; i < splits.size();) {
			try {
				i = addIndexedSplits(splits, i, newSplits, cfg);
			} catch (IOException e) {
				newSplits.add(
					getProbabilisticSplit((FileSplit)splits.get(i), cfg));
				++i;
			}
		}
		return newSplits;
	}

	// Handles all the splits that share the Path of the one at index i,
	// returning the next index to be used.
	private int addIndexedSplits(
			List<InputSplit> splits, int i, List<InputSplit> newSplits,
			Configuration cfg)
		throws IOException
	{
		final Path file = ((FileSplit)splits.get(i)).getPath();

		final SplittingBAMIndex idx = new SplittingBAMIndex(
			file.getFileSystem(cfg).open(getIdxPath(file)));

		int splitsEnd = splits.size();
		for (int j = i; j < splitsEnd; ++j)
			if (!file.equals(((FileSplit)splits.get(j)).getPath()))
				splitsEnd = j;

		for (int j = i; j < splitsEnd; ++j) {
			final FileSplit fileSplit = (FileSplit)splits.get(j);

			final long start =         fileSplit.getStart();
			final long end   = start + fileSplit.getLength();

			final Long blockStart =
				j == i ? idx.nextAlignment(0)
				       : idx.prevAlignment(start);
			final Long blockEnd =
				j == splitsEnd-1 ? idx.prevAlignment(end)
				                 : idx.nextAlignment(end);

			if (blockStart == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block start for " +start);

			if (blockEnd == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block end for " +end);

			newSplits.add(new FileVirtualSplit(
				file, blockStart, blockEnd, fileSplit.getLocations()));
		}
		return splitsEnd;
	}

	private FileVirtualSplit getProbabilisticSplit(
			FileSplit split, Configuration cfg)
		throws IOException
	{
		Path           path = split.getPath();
		SeekableStream sin  =
			WrapSeekable.openPath(path.getFileSystem(cfg), path);

		long beg =       split.getStart();
		long end = beg + split.getLength();

		long alignedBeg = BAMSplitGuesser.guessNextBAMRecordStart(sin, beg, end);

		return new FileVirtualSplit(
			path, alignedBeg, end << 16, split.getLocations());
	}
}
