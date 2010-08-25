// File created: 2010-08-25 11:24:30

package fi.tkk.ics.hadoop.bam.util;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import net.sf.samtools.util.SeekableStream;

// Hadoop and the SAM tools each have their own "seekable stream" abstraction.
// This class wraps Hadoop's so that we can give such a one to
// BlockCompressedInputStream and retain seekability.
public class WrapSeekable<S extends InputStream & Seekable>
	extends SeekableStream
{
	private final S    stm;
	private final long len;
	private final Path path;

	public WrapSeekable(final S s, long length, Path p) {
		stm  = s;
		len  = length;
		path = p;
	}

	@Override public String getSource() { return path.toString(); }
	@Override public long   length   () { return len; }

	@Override public void    close() throws IOException { stm.close(); }
	@Override public boolean eof  () throws IOException {
		return stm.getPos() == length();
	}
	@Override public void seek(long pos) throws IOException {
		stm.seek(pos);
	}
	@Override public int read() throws IOException {
		return stm.read();
	}
	@Override public int read(byte[] buf, int offset, int len)
		throws IOException
	{
		return stm.read(buf, offset, len);
	}
}
