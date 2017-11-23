// Copyright (c) 2011 Aalto University
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

// File created: 2011-01-17 15:17:59

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordHelper;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.FileTruncatedException;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.samtools.util.RuntimeIOException;

import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

/** A class for heuristically finding BAM record positions inside an area of
 * a BAM file.
 */
public class BAMSplitGuesser extends BaseSplitGuesser {
	private       SeekableStream             inFile;
	private       BlockCompressedInputStream bgzf;
	private final BAMRecordCodec             bamCodec;
	private final int                        referenceSequenceCount;
	private final SAMFileHeader              header;

	// We want to go through this many BGZF blocks fully, checking that they
	// contain valid BAM records, when guessing a BAM record position.
	private final static byte BLOCKS_NEEDED_FOR_GUESS = 3;

	// Since the max size of a BGZF block is 0xffff (64K), and we might be just
	// one byte off from the start of the previous one, we need 0xfffe bytes for
	// the start, and then 0xffff times the number of blocks we want to go
	// through.
	private final static int MAX_BYTES_READ =
		BLOCKS_NEEDED_FOR_GUESS * 0xffff + 0xfffe;

	private final static int SHORTEST_POSSIBLE_BAM_RECORD = 4*9 + 1 + 1 + 1;

	/** The stream must point to a valid BAM file, because the header is read
	 * from it.
	 */
	public BAMSplitGuesser(
			SeekableStream ss, Configuration conf)
		throws IOException
	{
		this(ss, ss, conf);

		// Secondary check that the header points to a BAM file: Picard can get
		// things wrong due to its autodetection.
		ss.seek(0);
		if (ss.read(buf.array(), 0, 4) != 4 || buf.getInt(0) != BGZF_MAGIC)
			throw new SAMFormatException("Does not seem like a BAM file");
	}

	public BAMSplitGuesser(
			SeekableStream ss, InputStream headerStream, Configuration conf)
		throws IOException
	{
		inFile = ss;

		header = SAMHeaderReader.readSAMHeaderFrom(headerStream, conf);
		referenceSequenceCount = header.getSequenceDictionary().size();

		bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory());
	}

	/** Finds a virtual BAM record position in the physical position range
	 * [beg,end). Returns end if no BAM record was found.
	 */
	public long guessNextBAMRecordStart(long beg, long end)
		throws IOException
	{
		// Use a reader to skip through the headers at the beginning of a BAM file, since
		// the headers may exceed MAX_BYTES_READ in length. Don't close the reader
		// otherwise it will close the underlying stream, which we continue to read from
		// on subsequent calls to this method.
		if (beg == 0) {
			this.inFile.seek(beg);
			SamReader open = SamReaderFactory.makeDefault().setUseAsyncIo(false)
					.open(SamInputResource.of(inFile));
			SAMFileSpan span = open.indexing().getFilePointerSpanningReads();
			if (span instanceof BAMFileSpan) {
				return ((BAMFileSpan) span).getFirstOffset();
			}
		}

		// Buffer what we need to go through.

		byte[] arr = new byte[MAX_BYTES_READ];

		this.inFile.seek(beg);
		int totalRead = 0;
		for (int left = Math.min((int)(end - beg), arr.length); left > 0;) {
			final int r = inFile.read(arr, totalRead, left);
			if (r < 0)
				break;
			totalRead += r;
			left -= r;
		}
		arr = Arrays.copyOf(arr, totalRead);

		this.in = new ByteArraySeekableStream(arr);

		this.bgzf = new BlockCompressedInputStream(this.in);
		this.bgzf.setCheckCrcs(true);

		this.bamCodec.setInputStream(bgzf);

		final int firstBGZFEnd = Math.min((int)(end - beg), 0xffff);

		// cp: Compressed Position, indexes the entire BGZF input.
		for (int cp = 0;; ++cp) {
			final PosSize psz = guessNextBGZFPos(cp, firstBGZFEnd);
			if (psz == null)
				return end;

			final int  cp0     = cp = psz.pos;
			final long cp0Virt = (long)cp0 << 16;
			try {
				bgzf.seek(cp0Virt);

			// This has to catch Throwable, because it's possible to get an
			// OutOfMemoryError due to an overly large size.
			} catch (Throwable e) {
				// Guessed BGZF position incorrectly: try the next guess.
				continue;
			}

			// up: Uncompressed Position, indexes the data inside the BGZF block.
			for (int up = 0;; ++up) {
				final int up0 = up = guessNextBAMPos(cp0Virt, up, psz.size);

				if (up0 < 0) {
					// No BAM records found in the BGZF block: try the next BGZF
					// block.
					break;
				}

				// Verify that we can actually decode BLOCKS_NEEDED_FOR_GUESS worth
				// of records starting at (cp0,up0).
				bgzf.seek(cp0Virt | up0);
				boolean decodedAny = false;
				try {
					byte b = 0;
					int prevCP = cp0;
					while (b < BLOCKS_NEEDED_FOR_GUESS)
					{
						SAMRecord record = bamCodec.decode();
						if (record == null) {
							break;
						}
						record.setHeaderStrict(header);
						SAMRecordHelper.eagerDecode(record); // force decoding of fields
						decodedAny = true;

						final int cp2 = (int)(bgzf.getFilePointer() >>> 16);
						if (cp2 != prevCP) {
							// The compressed position changed so we must be in a new
							// block.
							assert cp2 > prevCP;
							prevCP = cp2;
							++b;
						}
					}

					// Running out of records to verify is fine as long as we
					// verified at least something. It should only happen if we
					// couldn't fill the array.
					if (b < BLOCKS_NEEDED_FOR_GUESS) {
						assert arr.length < MAX_BYTES_READ;
						if (!decodedAny)
							continue;
					}
				}
                                  catch (SAMFormatException     e) { continue; }
				  catch (OutOfMemoryError       e) { continue; }
				  catch (IllegalArgumentException e) { continue; }
				  catch (IndexOutOfBoundsException e) { continue; }
				  catch (RuntimeIOException     e) { continue; }
				  // EOF can happen legitimately if the [beg,end) range is too
				  // small to accommodate BLOCKS_NEEDED_FOR_GUESS and we get cut
				  // off in the middle of a record. In that case, our stream
				  // should have hit EOF as well. If we've then verified at least
				  // something, go ahead with it and hope for the best.
				  catch (FileTruncatedException e) {
						if (!decodedAny && this.in.eof())
							continue;
				}
				  catch (RuntimeEOFException    e) {
						if (!decodedAny && this.in.eof())
							continue;
				}

				return beg+cp0 << 16 | up0;
			}
		}
	}

	private int guessNextBAMPos(long cpVirt, int up, int cSize) {
		// What we're actually searching for is what's at offset [4], not [0]. So
		// skip ahead by 4, thus ensuring that whenever we find a valid [0] it's
		// at position up or greater.
		up += 4;

		try {
			while (up + SHORTEST_POSSIBLE_BAM_RECORD - 4 < cSize) {
				bgzf.seek(cpVirt | up);
				IOUtils.readFully(bgzf, buf.array(), 0, 8);

				// If the first two checks fail we have what looks like a valid
				// reference sequence ID. Assume we're at offset [4] or [24], i.e.
				// the ID of either this read or its mate, respectively. So check
				// the next integer ([8] or [28]) to make sure it's a 0-based
				// leftmost coordinate.
				final int id  = buf.getInt(0);
				final int pos = buf.getInt(4);
				if (id < -1 || id > referenceSequenceCount || pos < -1) {
					++up;
					continue;
				}

				// Okay, we could be at [4] or [24]. Assuming we're at [4], check
				// that [24] is valid. Assume [4] because we should hit it first:
				// the only time we expect to hit [24] is at the beginning of the
				// split, as part of the first read we should skip.

				bgzf.seek(cpVirt | up+20);
				IOUtils.readFully(bgzf, buf.array(), 0, 8);

				final int nid  = buf.getInt(0);
				final int npos = buf.getInt(4);
				if (nid < -1 || nid > referenceSequenceCount || npos < -1) {
					++up;
					continue;
				}

				// So far so good: [4] and [24] seem okay. Now do something a bit
				// more involved: make sure that [36 + [12]&0xff - 1] == 0: that
				// is, the name of the read should be null terminated.

				// Move up to 0 just to make it less likely that we get confused
				// with offsets. Remember where we should continue from if we
				// reject this up.
				final int nextUP = up + 1;
				up -= 4;

				bgzf.seek(cpVirt | up+12);
				IOUtils.readFully(bgzf, buf.array(), 0, 4);

				final int nameLength = buf.getInt(0) & 0xff;
				if (nameLength < 1) {
					// Names are null-terminated so length must be at least one
					up = nextUP;
					continue;
				}

				final int nullTerminator = up + 36 + nameLength-1;

				if (nullTerminator >= cSize) {
					// This BAM record can't fit here. But maybe there's another in
					// the remaining space, so try again.
					up = nextUP;
					continue;
				}

				bgzf.seek(cpVirt | nullTerminator);
				IOUtils.readFully(bgzf, buf.array(), 0, 1);

				if (buf.get(0) != 0) {
					up = nextUP;
					continue;
				}

				// All of [4], [24], and [36 + [12]&0xff] look good. If [0] is also
				// sensible, that's good enough for us. "Sensible" to us means the
				// following:
				//
				// [0] >= 4*([16]&0xffff) + [20] + ([20]+1)/2 + 4*8 + ([12]&0xff)

				// Note that [0] is "length of the _remainder_ of the alignment
				// record", which is why this uses 4*8 instead of 4*9.
				int zeroMin = 4*8 + nameLength;

				bgzf.seek(cpVirt | up+16);
				IOUtils.readFully(bgzf, buf.array(), 0, 8);

				zeroMin += (buf.getInt(0) & 0xffff) * 4;
				zeroMin += buf.getInt(4) + (buf.getInt(4)+1)/2;

				bgzf.seek(cpVirt | up);
				IOUtils.readFully(bgzf, buf.array(), 0, 4);

				if (buf.getInt(0) < zeroMin) {
					up = nextUP;
					continue;
				}
				return up;
			}
		} catch (IOException e) {}
		return -1;
	}

	public static void main(String[] args) throws IOException {
		final GenericOptionsParser parser;
		try {
			parser = new GenericOptionsParser(args);

		// This should be IOException but Hadoop 0.20.2 doesn't throw it...
		} catch (Exception e) {
			System.err.printf("Error in Hadoop arguments: %s\n", e.getMessage());
			System.exit(1);

			// Hooray for javac
			return;
		}

		args = parser.getRemainingArgs();
                final Configuration conf = parser.getConfiguration();

		long beg = 0;

		if (args.length < 2 || args.length > 3) {
			System.err.println(
				"Usage: BAMSplitGuesser path-or-uri header-path-or-uri [beg]");
			System.exit(2);
		}

		try {
			if (args.length > 2) beg = Long.decode(args[2]);
		} catch (NumberFormatException e) {
			System.err.println("Invalid beg offset.");
			if (e.getMessage() != null)
				System.err.println(e.getMessage());
			System.exit(2);
		}

		SeekableStream ss = WrapSeekable.openPath(conf, new Path(args[0]));
		SeekableStream hs = WrapSeekable.openPath(conf, new Path(args[1]));

		final long end = beg + MAX_BYTES_READ;

		System.out.printf(
			"Will look for a BGZF block within: [%1$#x,%2$#x) = [%1$d,%2$d)\n"+
			"Will then verify BAM data within:  [%1$#x,%3$#x) = [%1$d,%3$d)\n",
			beg, beg + 0xffff, end);

		final long g =
			new BAMSplitGuesser(ss, hs, conf).guessNextBAMRecordStart(beg, end);

		ss.close();

		if (g == end) {
			System.out.println(
				"Didn't find any acceptable BAM record in any BGZF block.");
			System.exit(1);
		}

		System.out.printf(
			"Accepted BGZF block at offset %1$#x (%1$d).\n"+
			"Accepted BAM record at offset %2$#x (%2$d) therein.\n",
			g >> 16, g & 0xffff);
	}
}
