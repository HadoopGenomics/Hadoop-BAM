// File created: 2010-08-03 12:20:20

package fi.tkk.ics.hadoop.bam;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;

import net.sf.samtools.util.BlockCompressedInputStream;

public final class BAMIndexer {
	public static void main(String[] args) {
		if (args.length <= 1) {
			System.out.println(
				"Usage: BAMIndexer GRANULARITY [BAM files...]\n\n"+

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

		final BAMIndexer indexer = new BAMIndexer(granularity);

		for (final String arg : Arrays.asList(args).subList(1, args.length)) {
			final File f = new File(arg);
			if (f.isFile() && f.canRead()) {
				System.out.printf("Indexing %s...", f);
				try {
					indexer.index(f);
					System.out.println(" done.");
				} catch (IOException e) {
					System.out.println(" FAILED!");
					e.printStackTrace();
				}
			} else
				System.err.printf(
					"%s does not look like a file, won't index!\n", f);
		}
	}

	private final ByteBuffer byteBuffer;
	private final int granularity;

	private static final int PRINT_EVERY = 500*1024*1024;

	public BAMIndexer(int g) {
		granularity = g;
		byteBuffer = ByteBuffer.allocate(8); // Enough to fit a long
	}

	private void index(final File file) throws IOException {
		final BlockCompressedInputStream in =
			new BlockCompressedInputStream(file);

		final OutputStream out = new BufferedOutputStream(
			new FileOutputStream(file.getPath() + ".splitting-bai"));

		final LongBuffer lb =
			byteBuffer.order(ByteOrder.BIG_ENDIAN).asLongBuffer();

		skipToAlignmentList(in);

		// Always write the first one to make sure it's not skipped
		lb.put(0, in.getFilePointer());
		out.write(byteBuffer.array());

		long prevPrint = in.getFilePointer() >> 16;

		for (int i = 0;;) {
			final PtrSkipPair pair = readAlignment(in);
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
		lb.put(0, file.length() << 16);
		out.write(byteBuffer.array());
		out.close();
		in.close();
	}

	private void skipToAlignmentList(final InputStream in)
		throws IOException
	{
		// Check magic number
		if (!readExactlyBytes(in, 4))
			ioError("Invalid BAM header: too short, no magic");

		final int magic = byteBuffer.order(ByteOrder.BIG_ENDIAN).getInt(0);
		if (magic != 0x42414d01)
			ioError("Invalid BAM header: bad magic %#x != 0x42414d01", magic);

		// Skip the SAM header
		if (!readExactlyBytes(in, 4))
			ioError("Invalid BAM header: too short, no SAM header length");

		byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

		final int samLen = byteBuffer.getInt(0);
		if (samLen < 0)
			ioError("Invalid BAM header: negative SAM header length %d", samLen);

		fullySkip(in, samLen);

		// Get the number of reference sequences
		if (!readExactlyBytes(in, 4))
			ioError("Invalid BAM header: too short, no reference sequence count");

		final int referenceSeqs = byteBuffer.getInt(0);

		// Skip over each reference sequence datum individually
		for (int s = 0; s < referenceSeqs; ++s) {
			if (!readExactlyBytes(in, 4))
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

	private PtrSkipPair readAlignment(final BlockCompressedInputStream in)
		throws IOException
	{
		final long ptr = in.getFilePointer();
		final int read = readBytes(in, 4);
		if (read != 4) {
			if (read == 0)
				return null;
			ioError(
				"Invalid alignment at virtual offset %#x: "+
				"less than 4 bytes long", in.getFilePointer());
		}
		return new PtrSkipPair(ptr, byteBuffer.getInt(0));
	}

	private void fullySkip(final InputStream in, final int skip)
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

	private int readBytes(final InputStream in, final int n)
		throws IOException
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
	private boolean readExactlyBytes(final InputStream in, final int n)
		throws IOException
	{
		return readBytes(in, n) == n;
	}

	private void ioError(String s, Object... va) throws IOException {
		throw new IOException(String.format(s, va));
	}
}
