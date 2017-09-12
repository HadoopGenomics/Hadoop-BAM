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

// File created: 2011-11-15 11:58:23

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMRecord;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordFactory;

/**
 * A factory for the kind of lazy {@link BAMRecord} used internally.
 */
public class LazyBAMRecordFactory implements SAMRecordFactory {
    @Override
    public SAMRecord createSAMRecord(final SAMFileHeader hdr) {
        throw new UnsupportedOperationException(
                "LazyBAMRecordFactory can only create BAM records");
    }

    @Override
    public BAMRecord createBAMRecord(
            final SAMFileHeader hdr,
            final int referenceSequenceIndex, final int alignmentStart,
            final short readNameLength, final short mappingQuality,
            final int indexingBin, final int cigarLen, final int flags, final int readLen,
            final int mateReferenceSequenceIndex, final int mateAlignmentStart,
            final int insertSize, final byte[] variableLengthBlock) {
        return new LazyBAMRecord(
                hdr, referenceSequenceIndex, alignmentStart, readNameLength,
                mappingQuality, indexingBin, cigarLen, flags, readLen,
                mateReferenceSequenceIndex, mateAlignmentStart, insertSize,
                variableLengthBlock);
    }
}

class LazyBAMRecord extends BAMRecord {
    private boolean decodedRefIdx = false;
    private boolean decodedMateRefIdx = false;

    public LazyBAMRecord(
            final SAMFileHeader hdr, final int referenceID, final int coordinate,
            final short readNameLength, final short mappingQuality, final int indexingBin,
            final int cigarLen, final int flags, final int readLen,
            final int mateReferenceID, final int mateCoordinate, final int insertSize,
            final byte[] restOfData) {
        super(
                hdr, referenceID, coordinate, readNameLength, mappingQuality,
                indexingBin, cigarLen, flags, readLen, mateReferenceID,
                mateCoordinate, insertSize, restOfData);
    }

    @Override
    public void setReferenceIndex(final int referenceIndex) {
        mReferenceIndex = referenceIndex;
        decodedRefIdx = false;
    }

    @Override
    public void setMateReferenceIndex(final int referenceIndex) {
        mMateReferenceIndex = referenceIndex;
        decodedMateRefIdx = false;
    }

    @Override
    public String getReferenceName() {
        if (mReferenceIndex != null && !decodedRefIdx) {
            decodedRefIdx = true;
            super.setReferenceIndex(mReferenceIndex);
        }
        return super.getReferenceName();
    }

    @Override
    public String getMateReferenceName() {
        if (mMateReferenceIndex != null && !decodedMateRefIdx) {
            decodedMateRefIdx = true;
            super.setMateReferenceIndex(mMateReferenceIndex);
        }
        return super.getMateReferenceName();
    }

    @Override
    protected void eagerDecode() {
        getReferenceName();
        getMateReferenceName();
        super.eagerDecode();
    }

    @Override
    public boolean equals(final Object o) {
        // don't use decoded flags for equality check
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        // don't use decoded flags for hash code
        return super.hashCode();
    }
}
