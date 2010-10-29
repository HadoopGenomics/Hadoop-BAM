// Implements BAMRecordCodec.decode(), returning a LazyBAMRecord instead of a
// BAMRecord. Uses null in place of header (it's private to BAMRecordCodec
// anyway).
//
// Required because of LazyBAMRecord.

/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package fi.tkk.ics.hadoop.bam.custom.samtools;

import java.io.InputStream;

import net.sf.samtools.SAMFormatException;
import net.sf.samtools.util.BinaryCodec;
import net.sf.samtools.util.RuntimeEOFException;

public class LazyBAMRecordCodec extends BAMRecordCodec {
	protected final BinaryCodec binaryCodec = new BinaryCodec();

	public LazyBAMRecordCodec() { super(null); }

	@Override public void setInputStream(final InputStream is) {
		this.binaryCodec.setInputStream(is);
	}

	@Override public SAMRecord decode() {
        int recordLength = 0;
        try {
            recordLength = this.binaryCodec.readInt();
        }
        catch (RuntimeEOFException e) {
            return null;
        }

        if (recordLength < FIXED_BLOCK_SIZE ||
                recordLength > MAXIMUM_RECORD_LENGTH) {
            throw new SAMFormatException("Invalid record length: " + recordLength);
        }

        final int referenceID = this.binaryCodec.readInt();
        final int coordinate = this.binaryCodec.readInt() + 1;
        final short readNameLength = this.binaryCodec.readUByte();
        final short mappingQuality = this.binaryCodec.readUByte();
        final int bin = this.binaryCodec.readUShort();
        final int cigarLen = this.binaryCodec.readUShort();
        final int flags = this.binaryCodec.readUShort();
        final int readLen = this.binaryCodec.readInt();
        final int mateReferenceID = this.binaryCodec.readInt();
        final int mateCoordinate = this.binaryCodec.readInt() + 1;
        final int insertSize = this.binaryCodec.readInt();
        final byte[] restOfRecord = new byte[recordLength - FIXED_BLOCK_SIZE];
        this.binaryCodec.readBytes(restOfRecord);
        return new LazyBAMRecord(null, referenceID, coordinate, readNameLength, mappingQuality,
                bin, cigarLen, flags, readLen, mateReferenceID, mateCoordinate, insertSize, restOfRecord);
	}
}

