package org.elasticsearch.plugin.com.zhubl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;

public class JoinQueryResponse extends ActionResponse implements ToXContent {

    public SearchHits result;

    private Exception exception;

    JoinQueryResponse() {}

    public JoinQueryResponse(SearchHits result, Exception exception) {
        if( exception != null ){
            this.exception = exception;
        } else {
            this.result = result;
        }
    }

    public boolean isFailure() {
        return exception != null;
    }

    public Exception getFailure() {
        return exception;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.RESPONSES);
        builder.startObject();
        if (isFailure()) {
            ElasticsearchException.renderException(builder, params, getFailure());
            builder.field(Fields.STATUS, ExceptionsHelper.status(getFailure()).getStatus());
        } else{
            result.toXContent(builder, params);
        }
        builder.endObject();
        builder.endArray();
        return builder;
    }

    static final class Fields {
        static final String RESPONSES = "responses";
        static final String STATUS = "status";
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
