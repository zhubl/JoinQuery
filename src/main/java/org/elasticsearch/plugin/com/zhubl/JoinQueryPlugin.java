package org.elasticsearch.plugin.com.zhubl;


import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Collections;
import java.util.List;

public class JoinQueryPlugin extends Plugin implements ActionPlugin{
    public JoinQueryPlugin(){

    }

    public String name(){
        return "join-query";
    }

    public String description() {
        return "Elasticsearch plugin which supports join query between multiple indices .";
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Collections.singletonList(JoinQueryRestHandler.class);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(new ActionHandler<>(JoinQueryAction.INSTANCE, TransportJoinQueryAction.class));
    }
}
