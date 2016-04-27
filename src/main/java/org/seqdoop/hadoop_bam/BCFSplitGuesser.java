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

// File created: 2013-06-28 13:49:57

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;

import htsjdk.samtools.FileTruncatedException;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.readers.PositionalBufferedStream;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.vcf.VCFHeader;

import org.seqdoop.hadoop_bam.util.WrapSeekable;

/** A class for heuristically finding BCF record positions inside an area of
 * a BCF file. Handles both compressed and uncompressed BCF.
 */
public class BCFSplitGuesser extends BaseSplitGuesser {
	// cin is the compressed input: a BlockCompressedInputStream for compressed
	// BCF, otherwise equal to in. Unfortunately the closest common type is then
	// InputStream, which is why we have the cinSeek() method.
	private       InputStream    cin;
	private       SeekableStream inFile;
	private final boolean        bgzf;
	private final BCF2Codec      bcfCodec = new BCF2Codec();
	private final int            contigDictionaryLength, genotypeSampleCount;

	// The amount of data we verify for uncompressed BCF.
	private final static int UNCOMPRESSED_BYTES_NEEDED = 0x80000;

	// We want to go through this many BGZF blocks fully, checking that they
	// contain valid BCF records, when guessing a BCF record position.
	private final static byte BGZF_BLOCKS_NEEDED_FOR_GUESS = 2;

	// Since the max size of a BGZF block is 0xffff (64K), and we might be just
	// one byte off from the start of the previous one, we need 0xfffe bytes for
	// the start, and then 0xffff times the number of blocks we want to go
	// through.
	private final static int BGZF_MAX_BYTES_READ =
		BGZF_BLOCKS_NEEDED_FOR_GUESS * 0xffff + 0xfffe;

	// This is probably too conservative.
	private final static int SHORTEST_POSSIBLE_BCF_RECORD = 4*8 + 1;

	/** The stream must point to a valid BCF file, because the header is read
	 * from it.
	 */
	public BCFSplitGuesser(SeekableStream ss) throws IOException {
		this(ss, ss);
	}

	public BCFSplitGuesser(SeekableStream ss, InputStream headerStream)
		throws IOException
	{
		inFile = ss;

		InputStream bInFile = new BufferedInputStream(inFile);

		bgzf = BlockCompressedInputStream.isValidFile(bInFile);
		if (bgzf)
			bInFile = new BlockCompressedInputStream(bInFile);

		// Excess buffering here but it can't be helped that BCF2Codec only takes
		// PositionalBufferedStream.
		final VCFHeader header =
			(VCFHeader)bcfCodec.readHeader(
				new PositionalBufferedStream(bInFile)).getHeaderValue();

		contigDictionaryLength = header.getContigLines().size();
		genotypeSampleCount    = header.getNGenotypeSamples();
	}

	public boolean isBGZF() { return bgzf; }

	private void cinSeek(long virt) throws IOException {
		if (bgzf)
			((BlockCompressedInputStream)cin).seek(virt);
		else
			((SeekableStream)cin).seek(virt);
	}

	/** Finds a (virtual in the case of BGZF) BCF record position in the
	 * physical position range [beg,end). Returns end if no BCF record was
	 * found.
	 */
	public long guessNextBCFRecordStart(long beg, long end)
		throws IOException
	{
		// Buffer what we need to go through.

		byte[] arr = new byte[
			bgzf ? BGZF_MAX_BYTES_READ : UNCOMPRESSED_BYTES_NEEDED];

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

		final int firstBGZFEnd;

		if (this.bgzf) {
			firstBGZFEnd = Math.min((int)(end - beg), 0xffff);

			BlockCompressedInputStream bgzfStream =
				new BlockCompressedInputStream(this.in);
			bgzfStream.setCheckCrcs(true);
			this.cin = bgzfStream;
		} else {
			this.cin = this.in;

			firstBGZFEnd = 0; // Actually unused
		}

		// cp: Compressed Position, indexes the entire BGZF input. If
		// we have uncompressed BCF, this loop does nothing.
		for (int cp = 0;; ++cp) {

			final int  cp0;
			final long cp0Virt;
			final int  blockLen;

			if (this.bgzf) {
				final PosSize psz = guessNextBGZFPos(cp, firstBGZFEnd);
				if (psz == null)
					break;

				cp0 = cp = psz.pos;
				cp0Virt = (long)cp0 << 16;
				try {
					cinSeek(cp0Virt);

				// This has to catch Throwable, because it's possible to get an
				// OutOfMemoryError due to an overly large size.
				} catch (Throwable e) {
					// Guessed BGZF position incorrectly: try the next guess.
					continue;
				}
				blockLen = psz.size;
			} else {
				cp0 = 0; // Actually unused
				cp0Virt = 0;
				blockLen = Math.max(arr.length, UNCOMPRESSED_BYTES_NEEDED);
			}

			// up: Uncompressed Position, indexes the data inside the BGZF block.
			for (int up = 0;; ++up) {
				final int up0 = up = guessNextBCFPos(cp0Virt, up, blockLen);

				if (up0 < 0) {
					// No BCF records found in the BGZF block: try the next BGZF
					// block.
					break;
				}

				// Verification time.

				cinSeek(cp0Virt | up0);

				final PositionalBufferedStream pbIn =
					new PositionalBufferedStream(cin);

				boolean decodedAny = false;
				try {
					if (bgzf) {
						byte b = 0;
						int prevCP = cp0;
						while (b < BGZF_BLOCKS_NEEDED_FOR_GUESS && pbIn.peek() != -1)
						{
							bcfCodec.decode(pbIn);
							decodedAny = true;

							final int cp2 = (int)
								(((BlockCompressedInputStream)cin).getFilePointer()
								 >>> 16);
							if (cp2 != prevCP) {
								// The compressed position changed so we must be in a
								// new block.
								assert cp2 > prevCP;
								cp = cp2;
								++b;
							}
						}

						// Running out of records to verify is fine as long as we
						// verified at least something. It should only happen if we
						// couldn't fill the array.
						if (b < BGZF_BLOCKS_NEEDED_FOR_GUESS) {
							assert arr.length < BGZF_MAX_BYTES_READ;
							if (!decodedAny)
								continue;
						}
					} else {
						while (pbIn.getPosition() - up0 < UNCOMPRESSED_BYTES_NEEDED
						    && pbIn.peek() != -1)
						{
							bcfCodec.decode(pbIn);
							decodedAny = true;
						}

						// As in the BGZF case.
						if (pbIn.getPosition() - up0 < UNCOMPRESSED_BYTES_NEEDED) {
							assert arr.length < UNCOMPRESSED_BYTES_NEEDED;
							if (!decodedAny)
								continue;
						}
					}

				} catch (FileTruncatedException e) { continue; }
				  catch (OutOfMemoryError       e) { continue; }
				  catch (RuntimeEOFException    e) { continue; }
				  catch (TribbleException       e) {
					// This is the way in which BCF2Codec reports unexpected EOF.
					// Unfortunately, it also reports every other kind of error with
					// the same exception. It even wraps IOException in
					// TribbleException!
					//
					// We need to catch EOF in the middle of a record, which can
					// happen legitimately if the [beg,end) range is too small and
					// cuts off a record. First, require decodedAny, and then, assume
					// that this exception means EOF if the stream has hit EOF.
					if (!(decodedAny && pbIn.peek() == -1))
						continue;
				}

				return this.bgzf ? beg+cp0 << 16 | up0 : beg + up0;
			}
			if (!this.bgzf)
				break;
		}
		return end;
	}

	private int guessNextBCFPos(long cpVirt, int up, int cSize) {
		try {
			for (; up + SHORTEST_POSSIBLE_BCF_RECORD < cSize; ++up) {
				// Note! The BCF2 spec has a table listing the fields and their
				// types, but QUAL is misplaced there! It should be before
				// n_allele_info, not after n_fmt_sample! The "Putting it all
				// together" section shows the correct field order.

				// Check that [0] and [4] are big enough to make sense.

				cinSeek(cpVirt | up);
				IOUtils.readFully(cin, buf.array(), 0, 8);

				final long sharedLen = getUInt(0);
				final long  indivLen = getUInt(4);
				if (sharedLen + indivLen < (long)SHORTEST_POSSIBLE_BCF_RECORD)
					continue;

				// Check that [8] looks like a valid CHROM field and that [12] is a
				// 0-based leftmost coordinate.

				cinSeek(cpVirt | up+8);
				IOUtils.readFully(cin, buf.array(), 0, 8);

				final int chrom = buf.getInt(0);
				final int pos   = buf.getInt(4);
				if (chrom < 0 || chrom >= contigDictionaryLength || pos < 0)
					continue;

				// [24] and [26] are lengths and should thus be nonnegative.

				cinSeek(cpVirt | up+24);
				IOUtils.readFully(cin, buf.array(), 0, 4);
				final int alleleInfo = buf.getInt(0);

				final int alleleCount = alleleInfo >> 16;
				final int infoCount   = alleleInfo & 0xffff;
				if (alleleCount < 0) // don't check infoCount since it is always nonnegative
					continue;

				// Make sure that [28] matches to the same value in the header.

				cinSeek(cpVirt | up+28);
				IOUtils.readFully(cin, buf.array(), 0, 1);

				final short nSamples = getUByte(0);
				if ((int)nSamples != genotypeSampleCount)
					continue;

				// Check that the ID string has a sensible type encoding. That is,
				// it should claim to be a character string: [32] & 0x0f == 0x07.
				// Further, if it has length 15 or more, i.e. [32] & 0xf0 == 0xf0,
				// then it should be followed by an integer, i.e. [33] & 0x0f
				// should be in the range [1, 3], and the value of that integer
				// should be in the range [15, [0] - x) where x is the guaranteed
				// number of bytes in the first part of this record (before the
				// genotype block).

				cinSeek(cpVirt | up+32);
				IOUtils.readFully(cin, buf.array(), 0, 6);

				final byte idType = buf.get(0);
				if ((idType & 0x0f) != 0x07)
					continue;

				if ((idType & 0xf0) == 0xf0) {
					final byte idLenType = buf.get(1);
					final long idLen;
					switch (idLenType & 0x0f) {
					case 0x01: idLen = getUByte (2); break;
					case 0x02: idLen = getUShort(2); break;
					case 0x03: idLen = getUInt  (2); break;
					default: continue;
					}

					if (idLen < 15
					 || idLen > sharedLen - (4*8 + alleleCount + infoCount*2))
						continue;
				}

				// Good enough.
				return up;
			}
		} catch (IOException e) {
			// fall through
		}
		return -1;
	}
	private long getUInt(final int idx) {
		return (long)buf.getInt(idx) & 0xffffffff;
	}
	private short getUByte(final int idx) {
		return (short)((short)buf.get(idx) & 0xff);
	}

	public static void main(String[] args) throws IOException {
		final GenericOptionsParser parser;
		try {
			parser = new GenericOptionsParser(args);

		// This should be IOException but Hadoop 0.20.2 doesn't throw it...
		} catch (Exception e) {
			System.err.printf("Error in Hadoop arguments: %s\n", e.getMessage());
			System.exit(1);
			return;
		}

		args = parser.getRemainingArgs();
                final Configuration conf = parser.getConfiguration();

		long beg = 0;

		if (args.length < 2 || args.length > 3) {
			System.err.println(
				"Usage: BCFSplitGuesser path-or-uri header-path-or-uri [beg]");
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

		final BCFSplitGuesser guesser = new BCFSplitGuesser(ss, hs);
		final long end;

		if (guesser.isBGZF()) {
			end = beg + BGZF_MAX_BYTES_READ;

			System.out.printf(
				"This looks like a BGZF-compressed BCF file.\n"+
				"Will look for a BGZF block within: [%1$#x,%2$#x) = [%1$d,%2$d)\n"+
				"Will then verify BCF data within:  [%1$#x,%3$#x) = [%1$d,%3$d)\n",
				beg, beg + 0xffff, end);
		} else {
			end = beg + UNCOMPRESSED_BYTES_NEEDED;

			System.out.printf(
				"This looks like an uncompressed BCF file.\n"+
				"Will look for a BCF record within: [%1$#x,%2$#x) = [%1$d,%2$d)\n"+
				"And then will verify all following data in that range.\n",
				beg, end);
		}

		final long g = guesser.guessNextBCFRecordStart(beg, end);

		ss.close();

		if (g == end) {
			System.out.println(
				"Didn't find any acceptable BCF record in any BGZF block.");
			System.exit(1);
		}

		if (guesser.isBGZF())
			System.out.printf(
				"Accepted BGZF block at offset %1$#x (%1$d).\n"+
				"Accepted BCF record at offset %2$#x (%2$d) therein.\n",
				g >> 16, g & 0xffff);
		else
			System.out.printf("Accepted BCF record at offset %1$#x (%1$d).\n", g);
	}
}
