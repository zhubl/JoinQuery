package org.elasticsearch.plugin.com.zhubl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.SearchRequestParsers;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class JoinQueryRestHandler extends BaseRestHandler {

    private final SearchRequestParsers searchRequestParsers;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public JoinQueryRestHandler(Settings settings, RestController controller, SearchRequestParsers searchRequestParsers) {
        super(settings);
        this.searchRequestParsers = searchRequestParsers;

        controller.registerHandler(POST, "/_join", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        JoinQueryRequest jqRequest = parserJoinQueryRequest(request);
        JoinQueryExecutor jqExecutor = new JoinQueryExecutor();
        return channel -> jqExecutor.JoinQuery(client, jqRequest, new RestToXContentListener<>(channel));
    }

    private JoinQueryRequest parserJoinQueryRequest(RestRequest request) throws IOException {

        JoinQueryRequest jqRequest = new JoinQueryRequest();
        parserQueryRequests(request, jqRequest);
        jqRequest.addParserInfo(request.getXContentRegistry(), searchRequestParsers, parseFieldMatcher);
        return jqRequest;
    }

    private static void parserQueryRequests(RestRequest request, JoinQueryRequest joinQueryRequest) throws IOException {


        String json = XContentHelper.convertToJson(request.content(), true);
        byte[] data = json.getBytes();

        JsonNode rootNode = mapper.readTree(data);
        JsonNode point = rootNode;

        JsonNode tables = point.get("tables");
        JsonNode onKeys = point.get("on");

        int from = point.has("from") ? point.get("from").asInt() : 0;
        int size = point.has("size") ? point.get("size").asInt() : 10;

        Iterator<JsonNode> table = tables.elements();
        while(table.hasNext()){
            JsonNode info = table.next();
            joinQueryRequest.add(new JoinQueryInfo(info.get("index").asText(), info.get("type").asText(),
                info.get("content").toString().getBytes()));
        }

        Iterator<JsonNode> on = onKeys.elements();
        while(on.hasNext()){
            JsonNode onKey = on.next().get("keys");
            String[] onKeyInfoLeft = onKey.get(0).asText().split("\\.");
            String[] onKeyInfoRight = onKey.get(1).asText().split("\\.");

            joinQueryRequest.addJoinField(new JoinFields(new JoinFields.Fields(
                onKeyInfoLeft[0], onKeyInfoLeft[1], onKeyInfoLeft[2]),
                new JoinFields.Fields(onKeyInfoRight[0], onKeyInfoRight[1], onKeyInfoRight[2])
            ));
        }

        joinQueryRequest.setBaseIndexFrom(from);
        joinQueryRequest.setBaseIndexSize(size);

    }

}
