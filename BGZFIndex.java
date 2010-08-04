// File created: 2010-08-04 13:11:10

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.TreeSet;

public final class BGZFIndex {
	private final NavigableSet<Long> blockPositions = new TreeSet<Long>();

	public BGZFIndex() {}
	public BGZFIndex(final File path) throws IOException { readIndex(path); }

	public void readIndex(final File path) throws IOException {
		blockPositions.clear();

		final InputStream in =
			new BufferedInputStream(new FileInputStream(path));

		final ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putShort(0, (short)0); // Zero the two MSB that we don't need
		for (;;) {
			final int read = in.read(bb.array(), 2, 6);
			if (read < 6)
				break;
			blockPositions.add(bb.getLong(0));
		}
		in.close();
	}

	public Long blockOf(final long position) {
		return blockPositions.floor(position);
	}

	public int size() { return blockPositions.size(); }

	Long first() { return blockPositions.first(); }
	Long  last() { return blockPositions.last (); }

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println(
				"Usage: BGZFIndex [BGZF indices...]\n\n"+

				"Writes a few statistics about each BGZF index.");
			return;
		}

		for (String arg : args) {
			final File f = new File(arg);
			if (f.isFile() && f.canRead()) {
				try {
					System.err.printf("%s:\n", f);
					BGZFIndex bi = new BGZFIndex(f);
					System.err.printf(
						"\t%d blocks\n" +
						"\tfirst starts at %#014x\n" +
						"\tlast  ends   at %#014x (= file size %d)\n",
						bi.size(), bi.first(), bi.last(), bi.last());
				} catch (IOException e) {
					System.err.printf("Failed to read %s!\n", f);
					e.printStackTrace();
				}
			} else
				System.err.printf("%s does not look like a readable file!\n", f);
		}
	}
}
