// Copyright (c) 2012 Aalto University
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

// File created: 2012-03-08 14:58:13

package fi.tkk.ics.hadoop.bam.cli.plugins;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.util.BlockCompressedStreamConstants;

import fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser;
import static fi.tkk.ics.hadoop.bam.custom.jargs.gnu.CmdLineParser.Option.*;

import fi.tkk.ics.hadoop.bam.SAMFormat;
import fi.tkk.ics.hadoop.bam.cli.CLIPlugin;
import fi.tkk.ics.hadoop.bam.util.Pair;
import fi.tkk.ics.hadoop.bam.util.SAMOutputPreparer;

public final class Cat extends CLIPlugin {
	private static final List<Pair<CmdLineParser.Option, String>> optionDescs
		= new ArrayList<Pair<CmdLineParser.Option, String>>();

	private static final CmdLineParser.Option
		verboseOpt = new BooleanOption('v', "verbose");

	public Cat() {
		super("cat", "concatenation of partial SAM and BAM files", "1.0.1",
			"OUTPATH INPATH [INPATH...]",
			optionDescs,
			"Reads the SAM or BAM files in the given INPATHs and outputs "+
			"the reads contained within them directly to OUTPATH. Performs no "+
			"format conversions: simply concatenates the files. Note that BAM "+
			"files should not have terminator blocks in them, as they will also "+
			"be copied, which could confuse some tools."+
			"\n\n"+
			"Each INPATH can be a glob pattern as understood by Hadoop."+
			"\n\n"+
			"The header used in OUTPATH is selected from the first INPATH. The "+
			"output format is always the same as that of the INPATHs, and thus "+
			"is selected based on the first INPATH or its contents.");
	}
	static {
		optionDescs.add(new Pair<CmdLineParser.Option, String>(
			verboseOpt, "report progress verbosely"));
	}

	@Override protected int run(final CmdLineParser parser) {
		final List<String> args = parser.getRemainingArgs();
		if (args.isEmpty()) {
			System.err.println("cat :: OUTPATH not given.");
			return 3;
		}
		if (args.size() == 1) {
			System.err.println("cat :: no INPATHs given.");
			return 3;
		}

		final Path outPath = new Path(args.get(0));

		final List<String> ins = args.subList(1, args.size());

		final boolean verbose = parser.getBoolean(verboseOpt);

		final Configuration conf = getConf();

		// Expand the glob patterns.

		final List<Path> inputs = new ArrayList<Path>(ins.size());
		for (final String in : ins) {
			try {
				final Path p = new Path(in);
				for (final FileStatus fstat : p.getFileSystem(conf).globStatus(p))
					inputs.add(fstat.getPath());
			} catch (IOException e) {
				System.err.printf(
					"cat :: Could not expand glob pattern '%s': %s\n",
					in, e.getMessage());
			}
		}

		final Path input0 = inputs.get(0);

		// Infer the format from the first input path or contents.
		// the first input path or contents.

		SAMFormat format = SAMFormat.inferFromFilePath(input0);
		if (format == null) {
			try {
				format = SAMFormat.inferFromData(
					input0.getFileSystem(conf).open(input0));
			} catch (IOException e) {
				System.err.printf("cat :: Could not read input '%s': %s\n",
				                  input0, e.getMessage());
				return 4;
			}
			if (format == null) {
				System.err.printf(
					"cat :: Unknown SAM format in input '%s'\n", inputs.get(0));
				return 4;
			}
		}

		// Choose the header.

		final SAMFileHeader header;
		try {
			final SAMFileReader r = new SAMFileReader(
				input0.getFileSystem(conf).open(input0));

			header = r.getFileHeader();
			r.close();
		} catch (IOException e) {
			System.err.printf("cat :: Could not read input '%s': %s\n",
									input0, e.getMessage());
			return 5;
		}

		// Open the output.

		final OutputStream out;

		try {
			out = outPath.getFileSystem(conf).create(outPath);
		} catch (IOException e) {
			System.err.printf("cat :: Could not create output file: %s\n",
			                  e.getMessage());
			return 6;
		}

		// Output the header.

		try {
			// Don't use the returned stream, because we're concatenating directly
			// and don't want to apply another layer of compression to BAM.
			new SAMOutputPreparer().prepareForRecords(out, format, header);

		} catch (IOException e) {
			System.err.printf("cat :: Outputting header failed: %s\n",
									e.getMessage());
			return 7;
		}

		// Output the records from each file in the order given, converting if
		// necessary.

		int inIdx = 1;
		try { for (final Path inPath : inputs) {
			if (verbose) {
				System.out.printf("cat :: Concatenating path %d of %d...\n",
										inIdx++, inputs.size());
			}
			switch (format) {
				case SAM: {
					final InputStream in = inPath.getFileSystem(conf).open(inPath);

					// Use SAMFileReader to grab the header, but ignore it, thus
					// ensuring that the header has been skipped.
					new SAMFileReader(in).getFileHeader();

					IOUtils.copyBytes(in, out, conf, false);
					in.close();
					break;
				}
				case BAM: {
					final FSDataInputStream in =
						inPath.getFileSystem(conf).open(inPath);

					// Find the block length, thankfully given to us by the BGZF
					// format. We need it in order to know how much gzipped data to
					// read after skipping the BAM header, so that we can only read
					// that much and then simply copy the remaining gzip blocks
					// directly.

					final ByteBuffer block =
						ByteBuffer.wrap(new byte[0xffff])
						          .order(ByteOrder.LITTLE_ENDIAN);

					// Don't use readFully here, since EOF is fine.
					for (int read = 0, prev;
					     (prev = in.read(block.array(), read,
					                     block.capacity() - read))
					     	< block.capacity();)
					{
						// EOF is fine.
						if (prev == -1)
							break;
						read += prev;
					}

					// Find the BGZF subfield and extract the length from it.
					int blockLength = 0;
					for (int xlen = (int)block.getShort(10) & 0xffff,
					         i    = 12,
					         end  = i + xlen;
					         i < end;)
					{
						final int slen = (int)block.getShort(i+2) & 0xffff;
						if (block.getShort(i) == 0x4342 && slen == 2) {
							blockLength = ((int)block.getShort(i+4) & 0xffff) + 1;
							break;
						}
						i += 4 + slen;
					}
					if (blockLength == 0)
						throw new IOException(
							"BGZF extra field not found in " + inPath);

					if (verbose) {
						System.err.printf(
							"cat ::   first block length %d\n", blockLength);
					}

					// Skip the BAM header. Can't use SAMFileReader because it'll
					// use its own BlockCompressedInputStream.

					final ByteArrayInputStream blockIn =
						new ByteArrayInputStream(block.array(), 0, blockLength);

					final BlockCompressedInputStream bin =
						new BlockCompressedInputStream(blockIn);

					// Theoretically we could write into the ByteBuffer we already
					// had, since BlockCompressedInputStream needs to read the
					// header before it can decompress any data and thereafter we
					// can freely overwrite the first 8 bytes of the header... but
					// that's a bit too nasty, so let's not.
					final ByteBuffer buf =
						ByteBuffer.wrap(new byte[8]).order(ByteOrder.LITTLE_ENDIAN);

					// Read the BAM magic number and the SAM header length, verify
					// the magic, and skip the SAM header.

					IOUtils.readFully(bin, buf.array(), 0, 8);

					final int magic     = buf.getInt(0),
					          headerLen = buf.getInt(4);

					if (magic != 0x014d4142)
						throw new IOException("bad BAM magic number in " + inPath);

					IOUtils.skipFully(bin, headerLen);

					// Skip the reference sequences.

					IOUtils.readFully(bin, buf.array(), 0, 4);

					for (int i = buf.getInt(0); i-- > 0;) {
						// Read the reference name length and skip it along with the
						// reference length.
						IOUtils.readFully(bin, buf.array(), 0, 4);
						IOUtils.skipFully(bin, buf.getInt(0) + 4);
					}

					// Recompress the rest of this gzip block.

					final int remaining = bin.available();

					if (verbose)
						System.err.printf("cat ::   %d bytes to bgzip\n", remaining);

					if (remaining > 0) {
						// The overload of IOUtils.copyBytes that takes "long length"
						// was added only in Hadoop 0.20.205.0, which we don't want
						// to depend on, so copy manually.
						final byte[] remBuf = new byte[remaining];
						IOUtils.readFully(bin, remBuf, 0, remBuf.length);

						final BlockCompressedOutputStream bout =
							new BlockCompressedOutputStream(out, null);

						bout.write(remBuf);
						bout.flush();
					}

					// Just copy the raw bytes comprising the remaining blocks.

					in.seek(blockLength);
					IOUtils.copyBytes(in, out, conf, false);
					in.close();
					break;
				}
			}
		}} catch (IOException e) {
			System.err.printf("cat :: Outputting records failed: %s\n",
			                  e.getMessage());
			return 8;
		}

		// For BAM, output the BGZF terminator.

		try {
			if (format == SAMFormat.BAM)
				out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);

			out.close();
		} catch (IOException e) {
			System.err.printf("cat :: Finishing output failed: %s\n",
			                  e.getMessage());
			return 9;
		}
		return 0;
	}
}
