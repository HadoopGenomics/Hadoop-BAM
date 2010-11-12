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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private final Map<Path, SplittingBAMIndex> indices =
		new HashMap<Path, SplittingBAMIndex>();

	private Path getIdxPath(Path path) { return path.suffix(".splitting-bai"); }

	@Override public boolean isSplitable(JobContext job, Path path) {
		FileSystem fs;
		try {
			fs = FileSystem.get(job.getConfiguration());
			return getIndex(path, fs) != null;
		} catch (IOException e) {
			return false;
		}
	}

	/** The splits returned are FileVirtualSplits. */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		final List<InputSplit> splits = super.getSplits(job);

		// Align the splits so that they don't cross blocks

		final List<InputSplit> newSplits = new ArrayList<InputSplit>();

		final Configuration cfg = job.getConfiguration();

		for (int i = 0; i < splits.size(); ++i) {
			final FileSplit fileSplit = (FileSplit)splits.get(i);
			final Path file = fileSplit.getPath();

			final SplittingBAMIndex idx;
			try {
				idx = getIndex(file, file.getFileSystem(cfg));
			} catch (IOException e) {
				throw new IOException("No index, couldn't split", e);
			}

			final long start =         fileSplit.getStart();
			final long end   = start + fileSplit.getLength();

			final Long blockStart =
				i == 0 ? idx.nextAlignment(0)
				       : idx.prevAlignment(start);
			final Long blockEnd =
				i == splits.size()-1 ? idx.prevAlignment(end)
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
		return newSplits;
	}

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

	private SplittingBAMIndex getIndex(final Path path, final FileSystem fs)
		throws IOException
	{
		SplittingBAMIndex idx = indices.get(path);
		if (idx == null) {
			idx = new SplittingBAMIndex(fs.open(getIdxPath(path)));
			indices.put(path, idx);
		}
		return idx;
	}
}
