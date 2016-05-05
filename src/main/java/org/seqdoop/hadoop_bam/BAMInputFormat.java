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

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.DiskBasedBAMFileIndex;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import htsjdk.samtools.seekablestream.SeekableStream;

import org.apache.hadoop.conf.Configuration;
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
 */
public class BAMInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	// set this to true for debug output
	public final static boolean DEBUG_BAM_SPLITTER = false;

	/**
	 * If set to <code>true</code>, ensure that for paired reads both reads in a pair are
	 * always in the same split for queryname-sorted BAM files.
	 * <p>
	 * Note: only use this option if for all paired reads both reads in each pair are
	 * present, otherwise it is possible that reads may be silently dropped.
	 */
	public static final String KEEP_PAIRED_READS_TOGETHER_PROPERTY =
			"hadoopbam.bam.keep-paired-reads-together";

	/**
	 * Filter by region, like <code>-L</code> in SAMtools. Takes a comma-separated
	 * list of intervals, e.g. <code>chr1:1-20000,chr2:12000-20000</code>. For
	 * programmatic use {@link #setIntervals(Configuration, List)} should be preferred.
	 */
	public static final String INTERVALS_PROPERTY = "hadoopbam.bam.intervals";

	public static <T extends Locatable> void setIntervals(Configuration conf,
			List<T> intervals) {
		StringBuilder sb = new StringBuilder();
		for (Iterator<T> it = intervals.iterator(); it.hasNext(); ) {
			Locatable l = it.next();
			sb.append(String.format("%s:%d-%d", l.getContig(), l.getStart(), l.getEnd()));
			if (it.hasNext()) {
				sb.append(",");
			}
		}
		conf.set(INTERVALS_PROPERTY, sb.toString());
	}

	static List<Interval> getIntervals(Configuration conf) {
		String intervalsProperty = conf.get(INTERVALS_PROPERTY);
		if (intervalsProperty == null) {
			return null;
		}
		List<Interval> intervals = new ArrayList<Interval>();
		for (String s : intervalsProperty.split(",")) {
			String[] parts = s.split(":|-");
			Interval interval =
					new Interval(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
			intervals.add(interval);
		}
		return intervals;
	}

	private Path getIdxPath(Path path) {
		return path.suffix(SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
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

	/** The splits returned are {@link FileVirtualSplit FileVirtualSplits}. */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		return getSplits(super.getSplits(job), job.getConfiguration());
	}

	public List<InputSplit> getSplits(
			List<InputSplit> splits, Configuration cfg)
		throws IOException
	{
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

		for (int i = 0; i < splits.size();) {
			try {
				i = addIndexedSplits      (splits, i, newSplits, cfg);
			} catch (IOException e) {
				i = addProbabilisticSplits(splits, i, newSplits, cfg);
			}
		}
		return filterByInterval(newSplits, cfg);
	}

	// Handles all the splits that share the Path of the one at index i,
	// returning the next index to be used.
	private int addIndexedSplits(
			List<InputSplit> splits, int i, List<InputSplit> newSplits,
			Configuration cfg)
		throws IOException
	{
		final Path file = ((FileSplit)splits.get(i)).getPath();
		List<InputSplit> potentialSplits = new ArrayList<InputSplit>();

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

			final Long blockStart = idx.nextAlignment(start);

			// The last split needs to end where the last alignment ends, but the
			// index doesn't store that data (whoops); we only know where the last
			// alignment begins. Fortunately there's no need to change the index
			// format for this: we can just set the end to the maximal length of
			// the final BGZF block (0xffff), and then read until BAMRecordCodec
			// hits EOF.
			Long blockEnd;
			if (j == splitsEnd - 1) {
				blockEnd = idx.prevAlignment(end) | 0xffff;
			} else {
				blockEnd = idx.nextAlignment(end);
			}

			if (blockStart == null || blockEnd == null) {
				System.err.println("Warning: index for " + file.toString() +
						" was not good. Generating probabilistic splits.");

				return addProbabilisticSplits(splits, i, newSplits, cfg);
			}

			potentialSplits.add(new FileVirtualSplit(
						file, blockStart, blockEnd, fileSplit.getLocations()));
		}

		for (InputSplit s : potentialSplits) {
			newSplits.add(s);
		}
		return splitsEnd;
	}

	// Works the same way as addIndexedSplits, to avoid having to reopen the
	// file repeatedly and checking addIndexedSplits for an index repeatedly.
	private int addProbabilisticSplits(
			List<InputSplit> splits, int i, List<InputSplit> newSplits,
			Configuration cfg)
		throws IOException
	{
		final Path path = ((FileSplit)splits.get(i)).getPath();
		final SeekableStream sin =
			WrapSeekable.openPath(path.getFileSystem(cfg), path);

		final BAMSplitGuesser guesser = new BAMSplitGuesser(sin, cfg);

		FileVirtualSplit previousSplit = null;

		for (; i < splits.size(); ++i) {
			FileSplit fspl = (FileSplit)splits.get(i);
			if (!fspl.getPath().equals(path))
				break;

			long beg =       fspl.getStart();
			long end = beg + fspl.getLength();

			long alignedBeg = guesser.guessNextBAMRecordStart(beg, end);

			// As the guesser goes to the next BGZF block before looking for BAM
			// records, the ending BGZF blocks have to always be traversed fully.
			// Hence force the length to be 0xffff, the maximum possible.
			long alignedEnd = end << 16 | 0xffff;

			if (alignedBeg == end) {
				// No records detected in this split: merge it to the previous one.
				// This could legitimately happen e.g. if we have a split that is
				// so small that it only contains the middle part of a BGZF block.
				//
				// Of course, if it's the first split, then this is simply not a
				// valid BAM file.
				//
				// FIXME: In theory, any number of splits could only contain parts
				// of the BAM header before we start to see splits that contain BAM
				// records. For now, we require that the split size is at least as
				// big as the header and don't handle that case.
				if (previousSplit == null)
					throw new IOException("'" + path + "': "+
						"no reads in first split: bad BAM file or tiny split size?");

				previousSplit.setEndVirtualOffset(alignedEnd);
			} else {
				previousSplit = new FileVirtualSplit(
                                        path, alignedBeg, alignedEnd, fspl.getLocations());
				if(DEBUG_BAM_SPLITTER) {	
					final long byte_offset  = alignedBeg >>> 16;
                                	final long record_offset = alignedBeg & 0xffff;
					System.err.println("XXX split " + i +
						" byte offset: " + byte_offset + " record offset: " + 
						record_offset + " virtual offset: " + alignedBeg);
				}
				newSplits.add(previousSplit);
			}
		}

		sin.close();
		return i;
	}

	private List<InputSplit> filterByInterval(List<InputSplit> splits, Configuration conf)
			throws IOException {
		List<Interval> intervals = getIntervals(conf);
		if (intervals == null) {
			return splits;
		}

		// Get the chunks in the intervals we want (chunks give start and end file pointers
		// into a BAM file) by looking in all the indexes for the BAM files
		List<Chunk> chunks = new ArrayList<Chunk>();
		Set<Path> bamFiles = new LinkedHashSet<Path>();
		for (InputSplit split : splits) {
			bamFiles.add(((FileVirtualSplit) split).getPath());
		}
		for (Path bamFile : bamFiles) {
			Path indexFile = bamFile.suffix(BAMIndex.BAMIndexSuffix);
			FileSystem fs = bamFile.getFileSystem(conf);
			if (!fs.exists(indexFile)) {
				System.err.println("WARNING: no BAM index file found, splits will not be " +
						"filtered, which may be very inefficient: " + indexFile);
				return splits;
			}

			FSDataInputStream in = fs.open(bamFile);
			SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(in, conf);
			SAMSequenceDictionary dict = header.getSequenceDictionary();

			WrapSeekable seekableIdxFile = WrapSeekable.openPath(conf, indexFile);
			DiskBasedBAMFileIndex idx = new DiskBasedBAMFileIndex(seekableIdxFile, dict);

			for (Locatable interval : intervals) {
				int referenceIndex = dict.getSequenceIndex(interval.getContig());
				int intervalStart = interval.getStart();
				int intervalEnd = interval.getEnd();
				BAMFileSpan sfs = idx.getSpanOverlapping(referenceIndex, intervalStart, intervalEnd);
				chunks.addAll(sfs.getChunks());
			}
		}

		// Use the chunks to filter the splits
		List<InputSplit> filteredSplits = new ArrayList<InputSplit>();
		for (InputSplit split : splits) {
			FileVirtualSplit virtualSplit = (FileVirtualSplit) split;
			long splitStart = virtualSplit.getStartVirtualOffset();
			long splitEnd = virtualSplit.getEndVirtualOffset();
			// if any chunk overlaps with the split, keep the split, but adjust the start and
			// end to the maximally overlapping portion for all chunks that overlap
			long newStart = Long.MAX_VALUE;
			long newEnd = Long.MIN_VALUE;
			boolean overlaps = false;
			for (Chunk chunk : chunks) {
				long chunkStart = chunk.getChunkStart();
				long chunkEnd = chunk.getChunkEnd();
				if (overlaps(splitStart, splitEnd, chunkStart, chunkEnd)) {
					long overlapStart = Math.max(splitStart, chunkStart);
					long overlapEnd = Math.min(splitEnd, chunkEnd);
					newStart = Math.min(newStart, overlapStart);
					newEnd = Math.max(newEnd, overlapEnd);
					overlaps = true;
				}
			}
			if (overlaps) {
				filteredSplits.add(new FileVirtualSplit(virtualSplit.getPath(), newStart, newEnd,
						virtualSplit.getLocations()));
			}
		}
		return filteredSplits;
	}

	static boolean overlaps(long start, long end, long start2, long end2) {
		return (start2 >= start && start2 <= end) || (end2 >=start && end2 <= end) ||
				(start >= start2 && end <= end2);
	}

	@Override public boolean isSplitable(JobContext job, Path path) {
		return true;
	}
}
