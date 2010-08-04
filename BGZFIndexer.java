// File created: 2010-08-03 12:20:20

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import net.sf.samtools.util.BlockCompressedInputStream;

public final class BGZFIndexer {
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println(
				"Usage: BGZFIndexer [BGZF files...]\n\n"+

				"Writes, for each BGZF block in a file, its byte offset as a "+
				"big-endian 48-bit\ninteger into [filename].bgzf-index.");
			return;
		}

		BGZFIndexer indexer = new BGZFIndexer();

		for (String arg : args) {
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
	private       int        block; // Used only for error reporting

	public BGZFIndexer() {
		byteBuffer = ByteBuffer.allocate(8); // Enough to fit a long
	}

	private void index(final File file) throws IOException {
		final InputStream in = new BufferedInputStream(
			new FileInputStream(file));

		if (!BlockCompressedInputStream.isValidFile(in))
			throw new IOException("Does not look like a BGZF file");

		final OutputStream out = new BufferedOutputStream(
			new FileOutputStream(file.getPath() + ".bgzf-index"));

		final LongBuffer lb = byteBuffer.asLongBuffer();

		block = 0;
		for (long pos = 0;;) {
			++block;
			final LengthSkipPair pair = readBlockLength(in);

			if (pair == null)
				break;

			pos += pair.len;
			if (pos >= 1L << 48)
				throw new IOException(
					"File size exceeds 48 bits " +
					"(maximum supported by BAM virtual file offsets)");

			fullySkip(in, pair.skip);

			lb.put(0, pos);
			// Write only 6-byte chunks since we have the 48-bit limit anyway.
			// ByteBuffers are big-endian by default, so we can just drop the
			// first two bytes.
			out.write(byteBuffer.array(), 2, 6);
		}
		out.close();
		in.close();
	}

	private static final class LengthSkipPair {
		public int len, skip;

		public LengthSkipPair(int l, int s) {
			len  = l;
			skip = s;
		}
	}

	private LengthSkipPair readBlockLength(final InputStream in)
		throws IOException
	{
		// Check first 4 bytes' sensibility (id1, id2, compression method, flags)
		final int firstRead = readBytes(in, 4);
		if (firstRead != 4) {
			if (firstRead == 0)
				return null;
			ioError("Invalid header in block %d: too short, no magic", block);
		}

		final int magic = byteBuffer.order(ByteOrder.BIG_ENDIAN).getInt(0);
		if (magic != 0x1f8b0804)
			ioError("Invalid header in block %d: bad magic %#x", block, magic);

		// Get total byte length of extra subfields
		fullySkip(in, 10 - 4);
		if (!readBytesExactly(in, 2))
			ioError("Invalid header in block %d: too short, no xlen", block);

		byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		final int xlen = byteBuffer.getShort(0) & 0xffff;

		if (xlen < 6)
			ioError(
				"Invalid header in block %d: " +
				"missing BGZF subfield or corrupt xlen", block);

		// Skip subfields until we find the one we're looking for
		int read = 0;
		boolean found = false;

		for (int sf = 1; read < xlen; ++sf) {
			if (!readBytesExactly(in, 4))
				ioError(
					"Invalid header in block %d: EOF after subfield %d", block, sf);
			read += 4;

			final int sfid = byteBuffer.getShort(0) & 0xffff;
			final int slen = byteBuffer.getShort(2) & 0xffff;

			if (sfid == 0x4342 && slen == 2) {
				found = true;
				break;
			}

			fullySkip(in, slen);
			read += slen;
		}

		if (!found)
			ioError(
				"Invalid gzip header in block %d: missing BGZF subfield", block);

		// Get the total block size (minus 1) from the subfield
		if (!readBytesExactly(in, 2))
				ioError(
					"Invalid header in block %d: EOF in BGZF subfield", block);
		read += 2;

		final int blockSize = (byteBuffer.getShort(0) & 0xffff) + 1;

		// Calculate the number of bytes to skip and we're done.
		final int skip = xlen - read           // Any remaining subfields
		               + blockSize - xlen - 20 // The compressed data
		               + 4                     // CRC-32
		               + 4;                    // Uncompressed data length

		return new LengthSkipPair(blockSize, skip);
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
				throw new IOException("Corrupt block(?) " +block);
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
	private boolean readBytesExactly(final InputStream in, final int n)
		throws IOException
	{
		return readBytes(in, n) == n;
	}

	private void ioError(String s, Object... va) throws IOException {
		throw new IOException(String.format(s, va));
	}
}
