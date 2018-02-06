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

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import htsjdk.tribble.index.Block;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.util.TabixUtils;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import htsjdk.samtools.seekablestream.SeekableStream;

import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec;
import org.seqdoop.hadoop_bam.util.BGZFCodec;
import org.seqdoop.hadoop_bam.util.IntervalUtil;
import org.seqdoop.hadoop_bam.util.WrapSeekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for VCF files. Values
 * are the individual records; see {@link VCFRecordReader} for the meaning of
 * the key.
 */
public class VCFInputFormat
	extends FileInputFormat<LongWritable,VariantContextWritable>
{
	private static final Logger logger = LoggerFactory.getLogger(VCFInputFormat.class);

	/** Whether file extensions are to be trusted, defaults to true.
	 *
	 * @see VCFFormat#inferFromFilePath
	 */

	public static final String TRUST_EXTS_PROPERTY =
		"hadoopbam.vcf.trust-exts";

	/**
	 * Filter by region, like <code>-L</code> in SAMtools. Takes a comma-separated
	 * list of intervals, e.g. <code>chr1:1-20000,chr2:12000-20000</code>. For
	 * programmatic use {@link #setIntervals(Configuration, List)} should be preferred.
	 */
	public static final String INTERVALS_PROPERTY = "hadoopbam.vcf.intervals";

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
		return IntervalUtil.getIntervals(conf, INTERVALS_PROPERTY);
	}

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

		try(InputStream is = path.getFileSystem(conf).open(path)) {
			fmt = VCFFormat.inferFromData(is);
		} catch (IOException e) {}

		formatMap.put(path, fmt);
		return fmt;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		Configuration conf = context.getConfiguration();
		final CompressionCodec codec =
				new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
		if (codec == null) {
			return true;
		}
		if (codec instanceof BGZFCodec || codec instanceof BGZFEnhancedGzipCodec) {
			boolean splittable;
			try {
				try (FSDataInputStream in = filename.getFileSystem(conf).open(filename)) {
					splittable = BlockCompressedInputStream.isValidFile(new BufferedInputStream(in));
				}
			} catch (IOException e) {
				// can't determine if BGZF or GZIP, conservatively assume latter
				splittable = false;
			}
			if (!splittable) {
				logger.warn("{} is not splittable, consider using block-compressed gzip (BGZF)", filename);
			}
			return splittable;
		} else if (codec instanceof GzipCodec) {
			logger.warn("Using GzipCodec, which is not splittable, consider using block compressed gzip (BGZF) and BGZFCodec/BGZFEnhancedGzipCodec.");
		}
		return codec instanceof SplittableCompressionCodec;
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
			this.conf = ctx.getConfiguration();

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
			this.conf = job.getConfiguration();

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
		return filterByInterval(newSplits, conf);
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

	private List<InputSplit> filterByInterval(List<InputSplit> splits, Configuration conf)
			throws IOException {
		List<Interval> intervals = getIntervals(conf);
		if (intervals == null) {
			return splits;
		}
		List<Block> blocks = new ArrayList<>();
		Set<Path> vcfFiles = new LinkedHashSet<Path>();
		for (InputSplit split : splits) {
			if (split instanceof FileSplit) {
				vcfFiles.add(((FileSplit) split).getPath());
			} else if (split instanceof FileVirtualSplit) {
				vcfFiles.add(((FileVirtualSplit) split).getPath());
			} else {
				throw new IllegalArgumentException(
						"split '"+split+"' has unknown type: cannot extract path");
			}
		}
		for (Path vcfFile : vcfFiles) {
			Path indexFile = vcfFile.suffix(TabixUtils.STANDARD_INDEX_EXTENSION);
			FileSystem fs = vcfFile.getFileSystem(conf);
			if (!fs.exists(indexFile)) {
				logger.warn(
				        "No tabix index file found for {}, splits will not be filtered, which may be very inefficient",
                                        indexFile);
				return splits;
			}

			try (InputStream in = new BlockCompressedInputStream(fs.open(indexFile))) {
				TabixIndex index = new TabixIndex(in);
				for (Locatable interval : intervals) {
					String contig = interval.getContig();
					int intervalStart = interval.getStart();
					int intervalEnd = interval.getEnd();
					blocks.addAll(index.getBlocks(contig, intervalStart, intervalEnd));
				}
			}
		}

		// Use the blocks to filter the splits
		List<InputSplit> filteredSplits = new ArrayList<InputSplit>();
		for (InputSplit split : splits) {
			if (split instanceof FileSplit) {
				FileSplit fileSplit = (FileSplit) split;
				long splitStart = fileSplit.getStart() << 16;
				long splitEnd = (fileSplit.getStart() + fileSplit.getLength()) << 16;
				// if any block overlaps with the split, keep the split, but don't adjust its size
				// as the BGZF block decompression is handled by BGZFCodec, not by the reader
				// directly
				for (Block block : blocks) {
					long blockStart = block.getStartPosition();
					long blockEnd = block.getEndPosition();
					if (overlaps(splitStart, splitEnd, blockStart, blockEnd)) {
						filteredSplits.add(split);
						break;
					}
				}
			} else {
				FileVirtualSplit virtualSplit = (FileVirtualSplit) split;
				long splitStart = virtualSplit.getStartVirtualOffset();
				long splitEnd = virtualSplit.getEndVirtualOffset();
				// if any block overlaps with the split, keep the split, but adjust the start and
				// end to the maximally overlapping portion for all blocks that overlap
				long newStart = Long.MAX_VALUE;
				long newEnd = Long.MIN_VALUE;
				boolean overlaps = false;
				for (Block block : blocks) {
					long blockStart = block.getStartPosition();
					long blockEnd = block.getEndPosition();
					if (overlaps(splitStart, splitEnd, blockStart, blockEnd)) {
						long overlapStart = Math.max(splitStart, blockStart);
						long overlapEnd = Math.min(splitEnd, blockEnd);
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
		}
		return filteredSplits;
	}

	private static boolean overlaps(long start, long end, long start2, long end2) {
		return (start2 >= start && start2 <= end) || (end2 >=start && end2 <= end) ||
				(start >= start2 && end <= end2);
	}
}
