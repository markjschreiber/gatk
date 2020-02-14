package org.broadinstitute.hellbender.tools;

import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.util.Locatable;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.collections.HopscotchSet;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import picard.cmdline.programgroups.ReadDataManipulationProgramGroup;

import java.util.ArrayList;
import java.util.List;

@CommandLineProgramProperties(
        summary = "Prints read pairs within a region, along with their mates.",
        oneLineSummary = "Print read pairs within a region and their mates.",
        programGroup = ReadDataManipulationProgramGroup.class
)
@DocumentedFeature
public class PairWalker extends ReadWalker {
    private static final String MATE_CIGAR_TAG = "MC";

    private SimpleInterval targetInterval = null;
    private HopscotchSet<PairBuffer> pairBufferSet = new HopscotchSet<>(1000000);

    @Override public List<ReadFilter> getDefaultReadFilters() {
        final List<ReadFilter> readFilters = new ArrayList<>(super.getDefaultReadFilters());
        readFilters.add(ReadFilterLibrary.PAIRED);
        readFilters.add(ReadFilterLibrary.PRIMARY_LINE);
        readFilters.add(ReadFilterLibrary.NOT_DUPLICATE);
        return readFilters;
    }

    @Override
    public boolean requiresReads() { return true; }

    @Override
    public void apply( final GATKRead read,
                       final ReferenceContext referenceContext,
                       final FeatureContext featureContext ) {
        if ( targetInterval == null || targetInterval.overlaps(read) ) {
            store(read, true);
        } else {
            final Locatable mateLocation = getMateLocation(read);
            if ( targetInterval.overlaps(mateLocation) ) {
                store(read, true);
            } else if ( read.getContig() == null || mateLocation == null ) {
                store(read, false);
            }
        }
    }

    @Override
    public Object onTraversalSuccess() {
        System.out.println("There were " + pairBufferSet.size() + " unpaired reads.");
        return null;
    }

    public void apply( final GATKRead read, final GATKRead mate ) {
        //System.out.println(read.getName());
    }

    private void store( final GATKRead read, final boolean inInterval ) {
        final PairBuffer pb = pairBufferSet.findOrAdd(new PairBuffer(read.getName()), k -> (PairBuffer)k);
        if ( pb.add(read, inInterval) ) {
            apply( pb.getRead1(), pb.getRead2() );
            pairBufferSet.remove(pb);
        }
    }

    private static SimpleInterval getMateLocation( final GATKRead read ) {
        final String mateCigarString = read.getAttributeAsString("MC");
        if ( mateCigarString == null ) return null;
        final int mateLength = TextCigarCodec.decode(mateCigarString).getPaddedReferenceLength();
        return new SimpleInterval(read.getMateContig(), read.getMateStart(), read.getMateStart() + mateLength);
    }

    private static class PairBuffer {
        private final String qName;
        private GATKRead read1;
        private GATKRead read2;
        private boolean inInterval;

        public PairBuffer( final String qName ) {
            this.qName = qName;
            read1 = null;
            read2 = null;
            inInterval = false;
        }

        public GATKRead getRead1() { return read1; }
        public GATKRead getRead2() { return read2; }

        public boolean add( final GATKRead readArg, final boolean inInterval ) {
            final GATKRead read = DistantMateSortedPrinter.untangleRead(readArg);
            if ( read.isFirstOfPair() ) read1 = read;
            else read2 = read;
            return (this.inInterval |= inInterval) && read1 != null && read2 != null;
        }

        @Override public boolean equals( final Object obj ) {
            return this == obj || obj instanceof PairBuffer && qName.equals(((PairBuffer)obj).qName);
        }

        @Override public int hashCode() { return qName.hashCode(); }
    }
}
