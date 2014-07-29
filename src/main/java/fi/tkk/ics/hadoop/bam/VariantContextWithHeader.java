package fi.tkk.ics.hadoop.bam;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

public class VariantContextWithHeader extends VariantContext {
    private final VCFHeader header;

    public VariantContextWithHeader(VariantContext context, VCFHeader header) {
        super(context);
        this.header = header;
    }

    public VCFHeader getHeader() {
        return header;
    }
}
