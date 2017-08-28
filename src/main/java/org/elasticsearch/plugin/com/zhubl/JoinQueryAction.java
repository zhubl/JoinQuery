package org.elasticsearch.plugin.com.zhubl;


import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class JoinQueryAction extends Action<JoinQueryRequest, JoinQueryResponse, JoinQueryRequestBuilder> {

    public static final String NAME = "indices:data/read/joinquery";
    public static final JoinQueryAction INSTANCE = new JoinQueryAction();

    public JoinQueryAction() {
        super(NAME);
    }

    @Override
    public JoinQueryRequestBuilder newRequestBuilder(ElasticsearchClient client){
        return new JoinQueryRequestBuilder(client, INSTANCE);
    }

    @Override
    public JoinQueryResponse newResponse() {
        return new JoinQueryResponse();
    }
}
