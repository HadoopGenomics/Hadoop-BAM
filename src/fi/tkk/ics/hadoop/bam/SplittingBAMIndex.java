// File created: 2010-08-04 13:11:10

package fi.tkk.ics.hadoop.bam;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.TreeSet;

public final class SplittingBAMIndex {
	private final NavigableSet<Long> virtualOffsets = new TreeSet<Long>();

	public SplittingBAMIndex() {}
	public SplittingBAMIndex(final File path) throws IOException {
		this(new BufferedInputStream(new FileInputStream(path)));
	}
	public SplittingBAMIndex(final InputStream in) throws IOException {
		readIndex(in);
	}

	public void readIndex(final InputStream in) throws IOException {
		virtualOffsets.clear();

		final ByteBuffer bb = ByteBuffer.allocate(8);

		for (long prev = -1; in.read(bb.array()) == 8;) {
			final long cur = bb.getLong(0);
			if (prev > cur)
				throw new IOException(String.format(
					"Invalid splitting BAM index; offsets not in order: %#x > %#x",
					prev, cur));

			virtualOffsets.add(prev = cur);
		}
		in.close();

		if (virtualOffsets.size() < 2)
			throw new IOException(
				"Invalid splitting BAM index: "+
				"should contain at least 1 offset and the file size");
	}

	public Long prevAlignment(final long filePos) {
		return virtualOffsets.floor(filePos << 16);
	}
	public Long nextAlignment(final long filePos) {
		return virtualOffsets.higher(filePos << 16);
	}

	public int size() { return virtualOffsets.size(); }

	private long   first() { return virtualOffsets.first(); }
	private long    last() { return prevAlignment(bamSize() - 1); }
	private long bamSize() { return virtualOffsets.last() >>> 16; }

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println(
				"Usage: SplittingBAMIndex [splitting BAM indices...]\n\n"+

				"Writes a few statistics about each splitting BAM index.");
			return;
		}

		for (String arg : args) {
			final File f = new File(arg);
			if (f.isFile() && f.canRead()) {
				try {
					System.err.printf("%s:\n", f);
					final SplittingBAMIndex bi = new SplittingBAMIndex(f);
					final long first = bi.first();
					final long last  = bi.last();
					System.err.printf(
						"\t%d alignments\n" +
						"\tfirst is at %#06x in BGZF block at %#014x\n" +
						"\tlast  is at %#06x in BGZF block at %#014x\n" +
						"\tassociated BAM file size %d\n",
						bi.size(),
						first & 0xffff, first >>> 16,
						last  & 0xffff, last  >>> 16,
						bi.bamSize());
				} catch (IOException e) {
					System.err.printf("Failed to read %s!\n", f);
					e.printStackTrace();
				}
			} else
				System.err.printf("%s does not look like a readable file!\n", f);
		}
	}
}
