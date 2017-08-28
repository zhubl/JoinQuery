package org.elasticsearch.plugin.com.zhubl;


import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class JoinQueryRequestBuilder extends ActionRequestBuilder<JoinQueryRequest, JoinQueryResponse, JoinQueryRequestBuilder> {

    public JoinQueryRequestBuilder(ElasticsearchClient client, JoinQueryAction action)  {
        super(client, action, new JoinQueryRequest());
    }

}
