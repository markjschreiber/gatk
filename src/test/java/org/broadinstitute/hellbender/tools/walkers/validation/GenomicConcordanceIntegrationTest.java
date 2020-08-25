package org.broadinstitute.hellbender.tools.walkers.validation;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.engine.AbstractConcordanceWalker;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.testutils.VariantContextTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Takuto Sato on 1/31/17.
 */
public class GenomicConcordanceIntegrationTest extends CommandLineProgramTest{
    final double epsilon = 1e-3;

    private static final String CONCORDANCE_TEST_DIR = toolsTestDir + "concordance/";

    @Test
    public void testIdentical() throws Exception {
        final File evalVcf = new File(CONCORDANCE_TEST_DIR, "expected.testGVCFMode.gatk4.g.vcf");
        final File truthVcf = new File(CONCORDANCE_TEST_DIR, "expected.testGVCFMode.gatk4.g.vcf");
        final Path summary = createTempPath("summary", ".txt");
        final Path blockLengthHistogramFile = createTempPath("block_length_histogram", ".tsv");
        final Path confidenceHistogramFile = createTempPath("confidence_histogram", ".tsv");

        final String[] args = {
                "--" + AbstractConcordanceWalker.EVAL_VARIANTS_LONG_NAME, evalVcf.toString(),
                "--" + AbstractConcordanceWalker.TRUTH_VARIANTS_LONG_NAME, truthVcf.toString(),
                "--" + Concordance.SUMMARY_LONG_NAME, summary.toString(),
                "--" + GenomicConcordance.BLOCK_LENGTH_HISTOGRAM_LONG_NAME, blockLengthHistogramFile.toString(),
                "--" + GenomicConcordance.CONFIDENCE_HISTOGRAM_LONG_NAME, confidenceHistogramFile.toString(),
        };
        runCommandLine(args);

//        ConcordanceSummaryRecord.Reader reader = new ConcordanceSummaryRecord.Reader(summary);
//        ConcordanceSummaryRecord snpRecord = reader.readRecord();
//        ConcordanceSummaryRecord indelRecord = reader.readRecord();
//
//        Assert.assertEquals(snpRecord.getVariantType(), VariantContext.Type.SNP);
//        Assert.assertEquals(indelRecord.getVariantType(), VariantContext.Type.INDEL);
//
//        // numbers verified by manual inspection
//        Assert.assertEquals(snpRecord.getTruePositives(), 1);
//        Assert.assertEquals(indelRecord.getTruePositives(), 4);
//        Assert.assertEquals(snpRecord.getFalsePositives(), 0);
//        Assert.assertEquals(indelRecord.getFalsePositives(), 3);
//        Assert.assertEquals(snpRecord.getFalseNegatives(), 3);
//        Assert.assertEquals(indelRecord.getFalseNegatives(), 2);
//        Assert.assertEquals(snpRecord.getSensitivity(), 1.0/4, epsilon);
//        Assert.assertEquals(snpRecord.getPrecision(), 1.0, epsilon);

        // Test for block lengths
        GenomicConcordanceHistogramEntry.Reader blockLengthHistogramReader = new GenomicConcordanceHistogramEntry.Reader(blockLengthHistogramFile);
        blockLengthHistogramReader.forEach(entry -> Assert.assertEquals(entry.getTruthValue(), entry.getEvalValue()));

        // Test for GQ scores
        GenomicConcordanceHistogramEntry.Reader confidenceHistogramReader = new GenomicConcordanceHistogramEntry.Reader(confidenceHistogramFile);
        confidenceHistogramReader.forEach(entry -> Assert.assertEquals(entry.getTruthValue(), entry.getEvalValue()));
    }
}
