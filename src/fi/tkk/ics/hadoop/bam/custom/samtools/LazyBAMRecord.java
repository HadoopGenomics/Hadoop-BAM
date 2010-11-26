// Overrides SAMRecord.set[Mate]ReferenceIndex() to not calculate the reference
// name eagerly.
//
// Since we had to copy over SAMRecord anyway, its m[Mate]Reference{Index,Name}
// are protected instead of private, making this easier.
//
// Required because we can't always provide a header in Hadoop and just want to
// serialize/deserialize a BAM alignment as-is.

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

public class LazyBAMRecord extends BAMRecord {
	private boolean decodedRefIdx     = false;
	private boolean decodedMateRefIdx = false;

	public LazyBAMRecord(final SAMFileHeader header, final int referenceID, final int coordinate, final short readNameLength,
              final short mappingQuality, final int indexingBin, final int cigarLen, final int flags,
              final int readLen, final int mateReferenceID, final int mateCoordinate, final int insertSize,
              final byte[] restOfData) {
      super(header, referenceID, coordinate, readNameLength, mappingQuality, indexingBin, cigarLen, flags, readLen, mateReferenceID, mateCoordinate, insertSize, restOfData);
   }

	@Override public void setReferenceIndex(final int referenceIndex) {
		mReferenceIndex = referenceIndex;
	}
	@Override public void setMateReferenceIndex(final int referenceIndex) {
		mMateReferenceIndex = referenceIndex;
	}

	@Override public String getReferenceName() {
		if (mReferenceIndex != null && !decodedRefIdx) {
			decodedRefIdx = true;
			if (mReferenceIndex == NO_ALIGNMENT_REFERENCE_INDEX) {
				mReferenceName = NO_ALIGNMENT_REFERENCE_NAME;
			} else {
				try {
					mReferenceName = getHeader().getSequence(mReferenceIndex).getSequenceName();
				} catch (NullPointerException e) {
					throw new IllegalArgumentException("Reference index " + mReferenceIndex + " not found in sequence dictionary.", e);
				}
			}
		}
		return super.getReferenceName();
	}

	@Override public String getMateReferenceName() {
		if (mMateReferenceIndex != null && !decodedMateRefIdx) {
			decodedMateRefIdx = true;
			if (mMateReferenceIndex == NO_ALIGNMENT_REFERENCE_INDEX) {
				mMateReferenceName = NO_ALIGNMENT_REFERENCE_NAME;
			} else {
				try {
					mMateReferenceName = getHeader().getSequence(mMateReferenceIndex).getSequenceName();
				} catch (NullPointerException e) {
					throw new IllegalArgumentException("Reference index " + mMateReferenceIndex + " not found in sequence dictionary.", e);
				}
			}
		}
		return super.getMateReferenceName();
	}

	@Override protected void eagerDecode() {
		getReferenceName();
		getMateReferenceName();
		super.eagerDecode();
	}

}
