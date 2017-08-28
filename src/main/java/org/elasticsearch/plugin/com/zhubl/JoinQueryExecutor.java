package org.elasticsearch.plugin.com.zhubl;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;

/**
 * A wrapper of NodeClient
 */
public class JoinQueryExecutor {

    public void JoinQuery(NodeClient client, JoinQueryRequest request, ActionListener<JoinQueryResponse> listener) {
        client.execute(JoinQueryAction.INSTANCE, request, listener);
    }

}
