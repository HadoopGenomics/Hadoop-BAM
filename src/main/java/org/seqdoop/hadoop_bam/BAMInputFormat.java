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

import htsjdk.samtools.AbstractBAMFileIndex;
import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.LinearBAMIndex;
import htsjdk.samtools.LinearIndex;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.seqdoop.hadoop_bam.util.IntervalUtil;
import org.seqdoop.hadoop_bam.util.NIOFileUtil;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.ProviderNotFoundException;
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
	private static final Logger logger = LoggerFactory.getLogger(BAMInputFormat.class);

	/**
	 * If set to true, only include reads that overlap the given intervals (if specified),
	 * and unplaced unmapped reads (if specified). For programmatic use
	 * {@link #setTraversalParameters(Configuration, List, boolean)} should be preferred.
	 */
	public static final String BOUNDED_TRAVERSAL_PROPERTY = "hadoopbam.bam.bounded-traversal";

	/**
	 * If set to true, enables the use of BAM indices to calculate splits.
	 * For programmatic use
	 * {@link #setEnableBAISplitCalculator(Configuration, boolean)} should be preferred.
         * By default, this split calculator is disabled in favor of the splitting-bai calculator.
	 */
	public static final String ENABLE_BAI_SPLIT_CALCULATOR = "hadoopbam.bam.enable-bai-splitter";
    
	/**
	 * Filter by region, like <code>-L</code> in SAMtools. Takes a comma-separated
	 * list of intervals, e.g. <code>chr1:1-20000,chr2:12000-20000</code>. For
	 * programmatic use {@link #setIntervals(Configuration, List)} should be preferred.
	 */
	public static final String INTERVALS_PROPERTY = "hadoopbam.bam.intervals";

	/**
	 * If set to true, include unplaced unmapped reads (that is, unmapped reads with no
	 * position). For programmatic use
	 * {@link #setTraversalParameters(Configuration, List, boolean)} should be preferred.
	 */
	public static final String TRAVERSE_UNPLACED_UNMAPPED_PROPERTY = "hadoopbam.bam.traverse-unplaced-unmapped";

	/**
	 * If set to true, use the Intel inflater for decompressing DEFLATE compressed streams.
	 * If set, the <a href="https://github.com/Intel-HLS/GKL">GKL library</a> must be
	 * provided on the classpath.
	 */
	public static final String USE_INTEL_INFLATER_PROPERTY = "hadoopbam.bam.use-intel-inflater";

	/**
	 * Only include reads that overlap the given intervals. Unplaced unmapped reads are not
	 * included.
	 * @param conf the Hadoop configuration to set properties on
	 * @param intervals the intervals to filter by
	 * @param <T> the {@link Locatable} type
	 */
	public static <T extends Locatable> void setIntervals(Configuration conf,
			List<T> intervals) {
		setTraversalParameters(conf, intervals, false);
	}

        /**
         * Enables or disables the split calculator that uses the BAM index to calculate splits.
         */
        public static void setEnableBAISplitCalculator(Configuration conf,
                        boolean setEnabled) {
            conf.setBoolean(ENABLE_BAI_SPLIT_CALCULATOR, setEnabled);
        }

	/**
	 * Only include reads that overlap the given intervals (if specified) and unplaced
	 * unmapped reads (if <code>true</code>).
	 * @param conf the Hadoop configuration to set properties on
	 * @param intervals the intervals to filter by, or <code>null</code> if all reads
	 *   are to be included (in which case <code>traverseUnplacedUnmapped</code> must be
	 *   <code>true</code>)
	 * @param traverseUnplacedUnmapped whether to included unplaced unampped reads
	 * @param <T> the {@link Locatable} type
	 */
	public static <T extends Locatable> void setTraversalParameters(Configuration conf,
			List<T> intervals, boolean traverseUnplacedUnmapped) {
		if (intervals == null && !traverseUnplacedUnmapped) {
			throw new IllegalArgumentException("Traversing mapped reads only is not supported.");
		}
		conf.setBoolean(BOUNDED_TRAVERSAL_PROPERTY, true);
		if (intervals != null) {
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
		conf.setBoolean(TRAVERSE_UNPLACED_UNMAPPED_PROPERTY, traverseUnplacedUnmapped);
	}

	/**
	 * Reset traversal parameters so that all reads are included.
	 * @param conf the Hadoop configuration to set properties on
	 */
	public static void unsetTraversalParameters(Configuration conf) {
		conf.unset(BOUNDED_TRAVERSAL_PROPERTY);
		conf.unset(INTERVALS_PROPERTY);
		conf.unset(TRAVERSE_UNPLACED_UNMAPPED_PROPERTY);
	}

	static boolean isBoundedTraversal(Configuration conf) {
		return conf.getBoolean(BOUNDED_TRAVERSAL_PROPERTY, false) ||
				conf.get(INTERVALS_PROPERTY) != null; // backwards compatibility
	}

	static boolean traverseUnplacedUnmapped(Configuration conf) {
		return conf.getBoolean(TRAVERSE_UNPLACED_UNMAPPED_PROPERTY, false);
	}

	static List<Interval> getIntervals(Configuration conf) {
		return IntervalUtil.getIntervals(conf, INTERVALS_PROPERTY);
	}

	static boolean useIntelInflater(Configuration conf) {
		return conf.getBoolean(USE_INTEL_INFLATER_PROPERTY, false);
	}

	static Path getIdxPath(Path path) {
		return path.suffix(SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
	}

	static List<InputSplit> removeIndexFiles(List<InputSplit> splits) {
		// Remove any splitting bai files
		return splits.stream()
				.filter(split -> !((FileSplit) split).getPath().getName().endsWith(
						SplittingBAMIndexer.OUTPUT_FILE_EXTENSION))
                                .filter(split -> !((FileSplit) split).getPath().getName().endsWith(
                                                BAMIndex.BAMIndexSuffix))
				.collect(Collectors.toList());
        }
    
    	static Path getBAIPath(Path path) {
		return path.suffix(BAMIndex.BAMIndexSuffix);
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

		final List<InputSplit> origSplits = removeIndexFiles(splits);

		// Align the splits so that they don't cross blocks.

		// addIndexedSplits() requires the given splits to be sorted by file
		// path, so do so. Although FileInputFormat.getSplits() does, at the time
		// of writing this, generate them in that order, we shouldn't rely on it.
		Collections.sort(origSplits, new Comparator<InputSplit>() {
			public int compare(InputSplit a, InputSplit b) {
				FileSplit fa = (FileSplit)a, fb = (FileSplit)b;
				return fa.getPath().compareTo(fb.getPath());
			}
		});

		final List<InputSplit> newSplits =
			new ArrayList<InputSplit>(origSplits.size());

		for (int i = 0; i < origSplits.size();) {
			try {
				i = addIndexedSplits                        (origSplits, i, newSplits, cfg);
			} catch (IOException | ProviderNotFoundException e) {
				if (cfg.getBoolean(ENABLE_BAI_SPLIT_CALCULATOR, false)) {
					try {
						i = addBAISplits            (origSplits, i, newSplits, cfg);
					} catch (IOException | ProviderNotFoundException e2) {
						i = addProbabilisticSplits  (origSplits, i, newSplits, cfg);
					}
				} else {
					i = addProbabilisticSplits          (origSplits, i, newSplits, cfg);
				}
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

		if (idx.size() == 1) { // no alignments, only the file size, so no splits to add
			return splitsEnd;
		}

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
				logger.warn("Index for {} was not good. Generating probabilistic splits.", file);
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

	// Handles all the splits that share the Path of the one at index i,
	// returning the next index to be used.
        private int addBAISplits(List<InputSplit> splits,
                                 int i,
                                 List<InputSplit> newSplits,
                                 Configuration conf) throws IOException {
                int splitsEnd = i;

                final Path path = ((FileSplit)splits.get(i)).getPath();
                final Path baiPath = getBAIPath(path);
                final FileSystem fs = path.getFileSystem(conf);
                final Path sinPath;
                if (fs.exists(baiPath)) {
                    sinPath = baiPath;
                } else {
                    sinPath = new Path(path.toString().replaceFirst("\\.bam$", BAMIndex.BAMIndexSuffix));
                }
                try (final FSDataInputStream in = fs.open(path);
                     final SeekableStream guesserSin = WrapSeekable.openPath(fs, path);
                     final SeekableStream sin = WrapSeekable.openPath(fs, sinPath)) {

                        SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(in, conf);
                        SAMSequenceDictionary dict = header.getSequenceDictionary();
                        
                        final BAMSplitGuesser guesser = new BAMSplitGuesser(guesserSin, conf);

                        final LinearBAMIndex idx = new LinearBAMIndex(sin, dict);

			// searches for the first contig that contains linear bins
			// a contig will have no linear bins if there are no reads mapped to that
			// contig (e.g., reads were aligned to a whole genome, and then reads from
			// only a single contig were selected)
                        int ctgIdx = -1;
                        int bin = 0;
                        LinearIndex linIdx;
                        int ctgBins;
                        long lastStart = 0;
                        do {
                                ctgIdx++;
                                linIdx = idx.getLinearIndex(ctgIdx);
                                ctgBins = linIdx.size();
                        } while(ctgBins == 0);
                        long nextStart = linIdx.get(bin);
                        
                        FileVirtualSplit newSplit = null;
                        boolean lastWasGuessed = false;

			// loop and process all of the splits that share a single .bai
                        while(splitsEnd < splits.size() &&
                              ((FileSplit)(splits.get(splitsEnd))).getPath() == path) {
                                FileSplit fSplit = (FileSplit)splits.get(splitsEnd);
                                splitsEnd++;
                                
                                if (splitsEnd >= splits.size()) {
                                        break;
                                }

                                long fSplitEnd = (fSplit.getStart() + fSplit.getLength()) << 16;
                                lastStart = nextStart;

				// we need to advance and find the first linear index bin
				// that starts after the current split ends.
				// this is the end of our split.
				while(nextStart < fSplitEnd && ctgIdx < dict.size()) {

					// are we going off of the end of this contig?
					// if so, advance to the next contig with a linear bin
					if (bin + 1 >= ctgBins) {
						do {
                                                        ctgIdx += 1;
                                                        bin = 0;
                                                        if (ctgIdx >= dict.size()) {
                                                                break;
                                                        }
                                                        linIdx = idx.getLinearIndex(ctgIdx);
                                                        ctgBins = linIdx.size();
                                                } while (ctgBins == 0);
                                        }
                                        if (ctgIdx < dict.size() && linIdx.size() > bin) {
                                                nextStart = linIdx.get(bin);
                                                bin++;
                                        }
                                }

				// is this the first split?
				// if so, split ranges from where the reads start until the identified end
                                if (fSplit.getStart() == 0) {
                                        try (final SeekableStream inFile = WrapSeekable.openPath(path.getFileSystem(conf), path)) {
                                            SamReader open = SamReaderFactory.makeDefault().setUseAsyncIo(false)
                                                .open(SamInputResource.of(inFile));
                                            SAMFileSpan span = open.indexing().getFilePointerSpanningReads();
                                            long bamStart = ((BAMFileSpan) span).getFirstOffset();
                                            newSplit = new FileVirtualSplit(fSplit.getPath(),
                                                                        bamStart,
                                                                        nextStart - 1,
                                                                        fSplit.getLocations());
                                            newSplits.add(newSplit);
                                        }
                                } else {

					// did we find any blocks that started in the last split?
					// if yes, then we're fine
					// if no, then we need to guess a split start (in the else clause)
					if (lastStart != nextStart) {
                                                if (lastWasGuessed) {
                                                        newSplit.setEndVirtualOffset(lastStart - 1);
                                                        lastWasGuessed = false;
                                                }
                                                newSplit = new FileVirtualSplit(fSplit.getPath(),
                                                                                lastStart,
                                                                                nextStart - 1,
                                                                                fSplit.getLocations());
                                                newSplits.add(newSplit);
                                        } else {
						// guess the start
                                                long alignedBeg = guesser.guessNextBAMRecordStart(fSplit.getStart(),
                                                                                                  fSplit.getStart() + fSplit.getLength());
                                                newSplit.setEndVirtualOffset(alignedBeg - 1);
                                                lastStart = alignedBeg;
                                                nextStart = alignedBeg;
                                                newSplit = new FileVirtualSplit(fSplit.getPath(),
                                                                                alignedBeg,
                                                                                alignedBeg + 1,
                                                                                fSplit.getLocations());
                                                lastWasGuessed = true;
                                                newSplits.add(newSplit);
                                        }
                                }
                                lastStart = nextStart;
                        }
			// clean up the last split
                        if (splitsEnd == splits.size()) {
                                if (lastWasGuessed) {
                                        newSplit.setEndVirtualOffset(lastStart - 1);
                                        lastWasGuessed = false;
                                }
                                FileSplit fSplit = (FileSplit)splits.get(splitsEnd - 1);
                                long fSplitEnd = (fSplit.getStart() + fSplit.getLength()) << 16;
                                newSplit = new FileVirtualSplit(fSplit.getPath(),
                                                                lastStart,
                                                                fSplitEnd,
                                                                fSplit.getLocations());
                                newSplits.add(newSplit);
                        }
                }
                return splitsEnd + 1;
        }
        
	// Works the same way as addIndexedSplits, to avoid having to reopen the
	// file repeatedly and checking addIndexedSplits for an index repeatedly.
	private int addProbabilisticSplits(
			List<InputSplit> splits, int i, List<InputSplit> newSplits,
			Configuration cfg)
		throws IOException
	{
		final Path path = ((FileSplit)splits.get(i)).getPath();
        try (final SeekableStream sin = WrapSeekable.openPath(path.getFileSystem(cfg), path)) {

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
                    if (logger.isDebugEnabled()) {
                        final long byteOffset  = alignedBeg >>> 16;
                        final long recordOffset = alignedBeg & 0xffff;
                        logger.debug(
                            "Split {}: byte offset: {} record offset: {}, virtual offset: {}",
                            i, byteOffset, recordOffset, alignedBeg);
                    }
                    newSplits.add(previousSplit);
                }
            }
        }
        return i;
	}

	private List<InputSplit> filterByInterval(List<InputSplit> splits, Configuration conf)
			throws IOException {
		if (!isBoundedTraversal(conf)) {
			return splits;
		}

		// Get the chunk lists (BAMFileSpans) in the intervals we want (chunks give start
		// and end file pointers into a BAM file) by looking in all the indexes for the BAM
		// files
		Set<Path> bamFiles = new LinkedHashSet<>();
		for (InputSplit split : splits) {
			bamFiles.add(((FileVirtualSplit) split).getPath());
		}
		Map<Path, BAMFileSpan> fileToSpan = new LinkedHashMap<>();
		SamReaderFactory readerFactory = SamReaderFactory.makeDefault()
				.setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
				.setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
				.setUseAsyncIo(false);

		List<Interval> intervals = getIntervals(conf);

		Map<Path, Long> fileToUnmapped = new LinkedHashMap<>();
		boolean traverseUnplacedUnmapped = traverseUnplacedUnmapped(conf);

		for (Path bamFile : bamFiles) {
			FileSystem fs = bamFile.getFileSystem(conf);

			try (SamReader samReader =
							 readerFactory.open(NIOFileUtil.asPath(fs.makeQualified(bamFile).toUri()))) {
				if (!samReader.hasIndex()) {
					throw new IllegalArgumentException("Intervals set but no BAM index file found for " + bamFile);

				}

				try (FSDataInputStream in = fs.open(bamFile)) {
					SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(in, conf);
					SAMSequenceDictionary dict = header.getSequenceDictionary();
					BAMIndex idx = samReader.indexing().getIndex();

					if (intervals != null && !intervals.isEmpty()) {
						QueryInterval[] queryIntervals = prepareQueryIntervals(intervals, dict);
						fileToSpan.put(bamFile, BAMFileReader.getFileSpan(queryIntervals, idx));
					}

					if (traverseUnplacedUnmapped) {
						long startOfLastLinearBin = idx.getStartOfLastLinearBin();
						long noCoordinateCount = ((AbstractBAMFileIndex) idx).getNoCoordinateCount();
						if (startOfLastLinearBin != -1 && noCoordinateCount > 0) {
							// add FileVirtualSplit (with no intervals) from startOfLastLinearBin to
							// end of file
							fileToUnmapped.put(bamFile, startOfLastLinearBin);
						}
					}
				}

			}
		}

		// Use the chunks to filter the splits
		List<InputSplit> filteredSplits = new ArrayList<>();
		for (InputSplit split : splits) {
			FileVirtualSplit virtualSplit = (FileVirtualSplit) split;
			long splitStart = virtualSplit.getStartVirtualOffset();
			long splitEnd = virtualSplit.getEndVirtualOffset();
			BAMFileSpan splitSpan = new BAMFileSpan(new Chunk(splitStart, splitEnd));
			BAMFileSpan span = fileToSpan.get(virtualSplit.getPath());
			if (span == null) {
				continue;
			}
			span = (BAMFileSpan) span.removeContentsBefore(splitSpan);
			span = (BAMFileSpan) span.removeContentsAfter(splitSpan);
			if (!span.getChunks().isEmpty()) {
				filteredSplits.add(new FileVirtualSplit(virtualSplit.getPath(), splitStart, splitEnd,
						virtualSplit.getLocations(), span.toCoordinateArray()));
			}
		}

		if (traverseUnplacedUnmapped) {
			// add extra splits that contain only unmapped reads
			for (Map.Entry<Path, Long> e : fileToUnmapped.entrySet()) {
				Path file = e.getKey();
				long unmappedStart = e.getValue();
				boolean foundFirstSplit = false;
				for (InputSplit split : splits) { // TODO: are splits in order of start position?
					FileVirtualSplit virtualSplit = (FileVirtualSplit) split;
					if (virtualSplit.getPath().equals(file)) {
						long splitStart = virtualSplit.getStartVirtualOffset();
						long splitEnd = virtualSplit.getEndVirtualOffset();
						if (foundFirstSplit) {
							filteredSplits.add(new FileVirtualSplit(virtualSplit.getPath(), splitStart, splitEnd,
									virtualSplit.getLocations()));
						} else if (splitStart <= unmappedStart && unmappedStart <= splitEnd) {
							filteredSplits.add(new FileVirtualSplit(virtualSplit.getPath(), unmappedStart, splitEnd,
									virtualSplit.getLocations()));
							foundFirstSplit = true;
						}
					}
				}
			}
		}

		return filteredSplits;
	}

	/**
	 * Converts a List of SimpleIntervals into the format required by the SamReader query API
	 * @param rawIntervals SimpleIntervals to be converted
	 * @return A sorted, merged list of QueryIntervals suitable for passing to the SamReader query API
	 */
	static QueryInterval[] prepareQueryIntervals( final List<Interval>
			rawIntervals, final SAMSequenceDictionary sequenceDictionary ) {
		if ( rawIntervals == null || rawIntervals.isEmpty() ) {
			return null;
		}

		// Convert each SimpleInterval to a QueryInterval
		final QueryInterval[] convertedIntervals =
				rawIntervals.stream()
						.map(rawInterval -> convertSimpleIntervalToQueryInterval(rawInterval, sequenceDictionary))
						.toArray(QueryInterval[]::new);

		// Intervals must be optimized (sorted and merged) in order to use the htsjdk query API
		return QueryInterval.optimizeIntervals(convertedIntervals);
	}
	/**
	 * Converts an interval in SimpleInterval format into an htsjdk QueryInterval.
	 *
	 * In doing so, a header lookup is performed to convert from contig name to index
	 *
	 * @param interval interval to convert
	 * @param sequenceDictionary sequence dictionary used to perform the conversion
	 * @return an equivalent interval in QueryInterval format
	 */
	private static QueryInterval convertSimpleIntervalToQueryInterval( final Interval interval,	final SAMSequenceDictionary sequenceDictionary ) {
		if (interval == null) {
			throw new IllegalArgumentException("interval may not be null");
		}
		if (sequenceDictionary == null) {
			throw new IllegalArgumentException("sequence dictionary may not be null");
		}

		final int contigIndex = sequenceDictionary.getSequenceIndex(interval.getContig());
		if ( contigIndex == -1 ) {
			throw new IllegalArgumentException("Contig " + interval.getContig() + " not present in reads sequence " +
					"dictionary");
		}

		return new QueryInterval(contigIndex, interval.getStart(), interval.getEnd());
	}

	@Override public boolean isSplitable(JobContext job, Path path) {
		return true;
	}
}
