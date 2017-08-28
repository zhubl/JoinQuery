package org.elasticsearch.plugin.com.zhubl;



import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.elasticsearch.search.internal.InternalSearchHit.readSearchHit;


/**
 *
 */
public class JoinSearchHits implements SearchHits {

    public static JoinSearchHits empty() {
        // We shouldn't use static final instance, since that could directly be returned by native transport clients
        return new JoinSearchHits(EMPTY, 0);
    }

    public static final InternalSearchHit[] EMPTY = new InternalSearchHit[0];

    private SearchHit[] hits;

    public long totalHits;

    private float maxScore;

    private Exception exception = null;

    JoinSearchHits() {}

    public JoinSearchHits(SearchHit[] hits, long totalHits) {
        this.hits = hits;
        this.totalHits = totalHits;
    }

    public JoinSearchHits(Exception e){
        this.exception = e;
    }

    public boolean isFailure() {
        return exception != null;
    }

    public Exception getFailure() {
        return exception;
    }

    @Override
    public long totalHits() {
        return totalHits;
    }

    @Override
    public long getTotalHits() {
        return totalHits();
    }

    @Override
    public float maxScore() {
        return this.maxScore;
    }

    @Override
    public float getMaxScore() {
        return maxScore();
    }

    @Override
    public SearchHit[] hits() {
        return this.hits;
    }

    @Override
    public SearchHit getAt(int position) {
        return hits[position];
    }

    @Override
    public SearchHit[] getHits() {
        return hits();
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Arrays.stream(hits()).iterator();
    }

    static final class Fields {
        static final String HITS = "hits";
        static final String TOTAL = "total";
        static final String SOURCE = "_source";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HITS);
        builder.field(Fields.TOTAL, totalHits);
        builder.field(Fields.HITS);
        builder.startArray();
        for (SearchHit hit : hits) {
            hit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalHits = in.readVLong();
        maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new InternalSearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = readSearchHit(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHits);
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            for (SearchHit hit : hits) {
                hit.writeTo(out);
            }
        }
    }
}


