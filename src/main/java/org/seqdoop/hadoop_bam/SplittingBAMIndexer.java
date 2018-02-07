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

// File created: 2010-08-03 12:20:20

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMFileSource;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import htsjdk.samtools.util.BlockCompressedInputStream;

/**
 * An indexing tool and API for BAM files, making them palatable to {@link
 * org.seqdoop.hadoop_bam.BAMInputFormat}. Writes splitting BAM indices as
 * understood by {@link org.seqdoop.hadoop_bam.SplittingBAMIndex}.
 *
 * There are two ways of using this class:
 * 1) Building a splitting BAM index from an existing BAM file
 * 2) Building a splitting BAM index while building the BAM file
 *
 * For 1), use the static {@link #index(InputStream, OutputStream, long, int)} method,
 * which takes the input BAM and output stream to write the index to.
 *
 * For 2), use one of the constructors that takes an output stream, then pass {@link
 * SAMRecord} objects via the {@link #processAlignment} method, and then call {@link
 * #finish(long)} to complete writing the index.
 */
public final class SplittingBAMIndexer {
	public static final String OUTPUT_FILE_EXTENSION = ".splitting-bai";

	// Default to a granularity level of 4096. This is generally sufficient
	// for very large BAM files, relative to a maximum heap size in the
	// gigabyte range.
	public static final int DEFAULT_GRANULARITY = 4096;

	public static void main(String[] args) {
		if (args.length <= 1) {
			System.out.println(
				"Usage: SplittingBAMIndexer GRANULARITY [BAM files...]\n\n"+

				"Writes, for each GRANULARITY alignments in a BAM file, its "+
				"virtual file offset\nas a big-endian 64-bit integer into "+
				"[filename].splitting-bai. The file is\nterminated by the BAM "+
				"file's length, in the same format.");
			return;
		}

		int granularity;
		try {
			granularity = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			granularity = 0;
		}
		if (granularity <= 0) {
			System.err.printf(
				"Granularity must be a positive integer, not '%s'!\n", args[0]);
			return;
		}

		for (final String arg : Arrays.asList(args).subList(1, args.length)) {
			final File f = new File(arg);
			System.out.printf("Indexing %s...", f);
			try {
				SplittingBAMIndexer.index(
					new FileInputStream(f),
					new BufferedOutputStream(new FileOutputStream(f + OUTPUT_FILE_EXTENSION)),
					f.length(), granularity);
				System.out.println(" done.");
			} catch (IOException e) {
				System.out.println(" FAILED!");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Invoke a new SplittingBAMIndexer object, operating on the supplied {@link
	 * org.apache.hadoop.conf.Configuration} object instead of a supplied
	 * argument list
	 *
	 * @throws java.lang.IllegalArgumentException if the "input" property is not
	 *                                            in the Configuration
	 */
	public static void run(final Configuration conf) throws IOException {
		final String inputString = conf.get("input");
		if (inputString == null)
			throw new IllegalArgumentException(
				"String property \"input\" path not found in given Configuration");

		final FileSystem fs = FileSystem.get(conf);

		final Path input = new Path(inputString);

		SplittingBAMIndexer.index(
			fs.open(input),
			fs.create(input.suffix(OUTPUT_FILE_EXTENSION)),
			fs.getFileStatus(input).getLen(),
			conf.getInt("granularity", DEFAULT_GRANULARITY));
	}

	private final OutputStream out;
	private final ByteBuffer byteBuffer = ByteBuffer.allocate(8);
	private final int granularity;
	private final LongBuffer lb;
	private long count;
	private Method getFirstOffset;

	private static final int PRINT_EVERY = 500*1024*1024;

	/**
	 * Prepare to index a BAM file.
	 * @param out the stream to write the index to
	 */
	public SplittingBAMIndexer(final OutputStream out) {
		this(out, SplittingBAMIndexer.DEFAULT_GRANULARITY);
	}

	/**
	 * Prepare to index a BAM file.
	 * @param out the stream to write the index to
	 * @param granularity write the offset of every n-th alignment to the index
	 */
	public SplittingBAMIndexer(final OutputStream out, final int granularity) {
		this.out = out;
		this.lb = byteBuffer.order(ByteOrder.BIG_ENDIAN).asLongBuffer();
		this.granularity = granularity;
	}

	/**
	 * Process the given record for the index.
	 * @param rec the record from the file being indexed
	 * @throws IOException
	 */
	public void processAlignment(final SAMRecord rec) throws IOException {
		// write an offset for the first record and for the g-th record thereafter (where
		// g is the granularity), to be consistent with the index method
		if (count == 0 || (count + 1) % granularity == 0) {
			SAMFileSource fileSource = rec.getFileSource();
			SAMFileSpan filePointer = fileSource.getFilePointer();
			writeVirtualOffset(getPos(filePointer));
		}
		count++;
	}

	void processAlignment(final long virtualOffset) throws IOException {
		if (count == 0 || (count + 1) % granularity == 0) {
			writeVirtualOffset(virtualOffset);
		}
		count++;
	}

	private long getPos(SAMFileSpan filePointer) {
		// Use reflection since BAMFileSpan is package private in htsjdk 1.141. Note that
		// Hadoop-BAM cannot use a later version of htsjdk since it requires Java 8.
		if (getFirstOffset == null) {
			try {
				getFirstOffset = filePointer.getClass().getDeclaredMethod("getFirstOffset");
				getFirstOffset.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalStateException(e);
			}
		}
		try {
			return (Long) getFirstOffset.invoke(filePointer);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Write the given virtual offset to the index. This method is for internal use only.
	 * @param virtualOffset virtual file pointer
	 * @throws IOException
	 */
	public void writeVirtualOffset(long virtualOffset) throws IOException {
		lb.put(0, virtualOffset);
		out.write(byteBuffer.array());
	}

	/**
	 * Complete the index by writing the input BAM file size to the index, and closing
	 * the output stream.
	 * @param inputSize the size of the input BAM file
	 * @throws IOException
	 */
	public void finish(long inputSize) throws IOException {
		writeVirtualOffset(inputSize << 16);
		out.close();
	}

	/**
	 * Perform indexing on the given BAM file, at the granularity level specified.
	 */
	public static void index(
			final InputStream rawIn, final OutputStream out, final long inputSize,
			final int granularity)
		throws IOException
	{
		final BlockCompressedInputStream in =
			new BlockCompressedInputStream(rawIn);

		final ByteBuffer byteBuffer = ByteBuffer.allocate(8); // Enough to fit a long
		final LongBuffer lb =
			byteBuffer.order(ByteOrder.BIG_ENDIAN).asLongBuffer();

		skipToAlignmentList(byteBuffer, in);

		// Always write the first one to make sure it's not skipped
		lb.put(0, in.getFilePointer());
		out.write(byteBuffer.array());

		long prevPrint = in.getFilePointer() >> 16;

		for (int i = 0;;) {
			final PtrSkipPair pair = readAlignment(byteBuffer, in);
			if (pair == null)
				break;

			if (++i == granularity) {
				i = 0;
				lb.put(0, pair.ptr);
				out.write(byteBuffer.array());

				final long filePos = pair.ptr >> 16;
				if (filePos - prevPrint >= PRINT_EVERY) {
					System.out.print("-");
					prevPrint = filePos;
				}
			}
			fullySkip(in, pair.skip);
		}
		lb.put(0, inputSize << 16);
		out.write(byteBuffer.array());
		out.close();
		in.close();
	}

	private static void skipToAlignmentList(final ByteBuffer byteBuffer, final InputStream in)
			throws IOException {
		// Check magic number
		if (!readExactlyBytes(byteBuffer, in, 4))
			ioError("Invalid BAM header: too short, no magic");

		final int magic = byteBuffer.order(ByteOrder.BIG_ENDIAN).getInt(0);
		if (magic != 0x42414d01)
			ioError("Invalid BAM header: bad magic %#x != 0x42414d01", magic);

		// Skip the SAM header
		if (!readExactlyBytes(byteBuffer, in, 4))
			ioError("Invalid BAM header: too short, no SAM header length");

		byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

		final int samLen = byteBuffer.getInt(0);
		if (samLen < 0)
			ioError("Invalid BAM header: negative SAM header length %d", samLen);

		fullySkip(in, samLen);

		// Get the number of reference sequences
		if (!readExactlyBytes(byteBuffer, in, 4))
			ioError("Invalid BAM header: too short, no reference sequence count");

		final int referenceSeqs = byteBuffer.getInt(0);

		// Skip over each reference sequence datum individually
		for (int s = 0; s < referenceSeqs; ++s) {
			if (!readExactlyBytes(byteBuffer, in, 4))
				ioError("Invalid reference list: EOF before reference %d", s+1);

			// Skip over the name + the int giving the sequence length
			fullySkip(in, byteBuffer.getInt(0) + 4);
		}
	}

	private static final class PtrSkipPair {
		public long ptr;
		public int skip;

		public PtrSkipPair(long p, int s) {
			ptr  = p;
			skip = s;
		}
	}

	private static PtrSkipPair readAlignment(final ByteBuffer byteBuffer,
			final BlockCompressedInputStream in) throws IOException
	{
		final long ptr = in.getFilePointer();
		final int read = readBytes(byteBuffer, in, 4);
		if (read != 4) {
			if (read == 0)
				return null;
			ioError(
				"Invalid alignment at virtual offset %#x: "+
				"less than 4 bytes long", in.getFilePointer());
		}
		return new PtrSkipPair(ptr, byteBuffer.getInt(0));
	}

	private static void fullySkip(final InputStream in, final int skip)
		throws IOException
	{
		// Skip repeatedly until we're either done skipping or can't skip any
		// more, in case some kind of IO error is temporarily preventing it. That
		// kind of situation might not necessarily be possible; the docs are
		// rather vague about the whole thing.
		for (int s = skip; s > 0;) {
			final long skipped = in.skip(s);
			if (skipped == 0)
				throw new IOException("Skip failed");
			s -= skipped;
		}
	}

	private static int readBytes(final ByteBuffer byteBuffer, final InputStream in,
			final int n) throws IOException
	{
		assert n <= byteBuffer.capacity();

		int read = 0;
		while (read < n) {
			final int readNow = in.read(byteBuffer.array(), read, n - read);
			if (readNow <= 0)
				break;
			read += readNow;
		}
		return read;
	}
	private static boolean readExactlyBytes(final ByteBuffer byteBuffer,
			final InputStream in, final int n) throws IOException
	{
		return readBytes(byteBuffer, in, n) == n;
	}

	private static void ioError(String s, Object... va) throws IOException {
		throw new IOException(String.format(s, va));
	}
}
