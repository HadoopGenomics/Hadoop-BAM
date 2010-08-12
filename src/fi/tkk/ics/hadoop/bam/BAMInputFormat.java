// File created: 2010-08-03 11:50:19

package fi.tkk.ics.hadoop.bam;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BAMInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	private final Map<Path, SplittingBAMIndex> indices =
		new HashMap<Path, SplittingBAMIndex>();

	private Path getIdxPath(Path path) { return path.suffix(".splitting-bai"); }

	@Override protected boolean isSplitable(JobContext job, Path path) {
		FileSystem fs;
		try {
			fs = FileSystem.get(job.getConfiguration());
		} catch (IOException e) {
			return false;
		}
		return getIndex(path, fs) != null;
	}

	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		final List<InputSplit> splits = super.getSplits(job);

		// Align the splits so that they don't cross blocks

		final List<InputSplit> newSplits = new ArrayList<InputSplit>();

		final FileSystem fs = FileSystem.get(job.getConfiguration());

		for (int i = 0; i < splits.size(); ++i) {
			final FileSplit fileSplit = (FileSplit)splits.get(i);
			final Path file = fileSplit.getPath();

			final SplittingBAMIndex idx = getIndex(file, fs);
			if (idx == null)
				throw new IOException("No index, couldn't split");

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

	@Override public RecordReader<LongWritable,SAMRecordWritable>
		createRecordReader(
			InputSplit split, TaskAttemptContext ctx)
		throws InterruptedException, IOException
	{
		final RecordReader<LongWritable,SAMRecordWritable> rr =
			new BAMRecordReader();
		rr.initialize(split, ctx);
		return rr;
	}

	private SplittingBAMIndex getIndex(final Path path, final FileSystem fs) {
		SplittingBAMIndex idx = indices.get(path);
		if (idx == null && !indices.containsKey(path)) {
			try {
				idx = new SplittingBAMIndex(fs.open(getIdxPath(path)));
			} catch (IOException e) {
				idx = null;
			}
			indices.put(path, idx);
		}
		return idx;
	}
}
