package fi.tkk.ics.hadoop.bam;

import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.vcf.VCFHeader;

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
