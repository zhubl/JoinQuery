package org.elasticsearch.plugin.com.zhubl;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;


public class TransportJoinQueryAction extends HandledTransportAction<JoinQueryRequest, JoinQueryResponse> implements ActionPlugin {

    private final ClusterService clusterService;
    private final TransportAction<SearchRequest, SearchResponse> searchAction;

    @Inject
    public TransportJoinQueryAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                    ClusterService clusterService, TransportSearchAction searchAction,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, JoinQueryAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, JoinQueryRequest::new);
        this.clusterService = clusterService;
        this.searchAction = searchAction;
    }

    @Override
    public void doExecute(JoinQueryRequest request, ActionListener<JoinQueryResponse> listener) {
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        executeSearch(request.getFirst(), listener, request);

    }

    void executeSearch(SearchRequest searchRequest, ActionListener<JoinQueryResponse> listener,
                       JoinQueryRequest joinQueryRequest) {
        searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse){
                SearchRequest nextSearchRequest = joinQueryRequest.getNext(searchResponse);
                if(nextSearchRequest != null){
                    executeSearch(nextSearchRequest, listener, joinQueryRequest);
                }
                else{
                    JoinSearchHits resultHits = JoinQueryHelper.GetJoinSearchHits(joinQueryRequest.nowSearchHits());
                    listener.onResponse(new JoinQueryResponse(resultHits,null));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onResponse(new JoinQueryResponse(null, e));
            }
        });
    }

}
