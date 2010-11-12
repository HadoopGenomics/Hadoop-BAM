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

package fi.tkk.ics.hadoop.bam.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for BGZF-compressed
 * files.
 *
 * <p>A {@link BGZFBlockIndex} for each Path used is required, or an
 * <code>IOException</code> is thrown out of {@link #getSplits}.</p>
 */
public abstract class BGZFSplitFileInputFormat<K,V>
	extends FileInputFormat<K,V>
{
	private final Map<Path, BGZFBlockIndex> indices =
		new HashMap<Path, BGZFBlockIndex>();

	private Path getIdxPath(Path path) { return path.suffix(".bgzfi"); }

	@Override public boolean isSplitable(JobContext job, Path path) {
		FileSystem fs;
		try {
			fs = FileSystem.get(job.getConfiguration());
			return getIndex(path, fs) != null;
		} catch (IOException e) {
			return false;
		}
	}

	/** The splits returned are FileSplits. */
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

			final BGZFBlockIndex idx;
			try {
				idx = getIndex(file, file.getFileSystem(cfg));
			} catch (IOException e) {
				throw new IOException("No index, couldn't split", e);
			}

			final long start =         fileSplit.getStart();
			final long end   = start + fileSplit.getLength();

			final Long blockStart = idx.prevBlock(start);
			final Long blockEnd =
				i == splits.size()-1 ? idx.prevBlock(end)
				                     : idx.nextBlock(end);

			if (blockStart == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block start for " +start);

			if (blockEnd == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block end for " +end);

			newSplits.add(new FileSplit(
				file, blockStart, blockEnd - blockStart,
				fileSplit.getLocations()));
		}
		return newSplits;
	}

	private BGZFBlockIndex getIndex(final Path path, final FileSystem fs)
		throws IOException
	{
		BGZFBlockIndex idx = indices.get(path);
		if (idx == null && !indices.containsKey(path)) {
			idx = new BGZFBlockIndex(fs.open(getIdxPath(path)));
			indices.put(path, idx);
		}
		return idx;
	}
}
