package org.elasticsearch.plugin.com.zhubl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class JoinQueryRequest extends ActionRequest implements CompositeIndicesRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private NamedXContentRegistry requestXContentRegistry;
    private SearchRequestParsers searchRequestParsers;
    private ParseFieldMatcher parseFieldMatcher;

    private List<JoinFields> joinFields = new ArrayList<>();

    private List<JoinQueryInfo> joinQueryInfos = new ArrayList<>();

    private List<Map<String, Object>> nowSearchHits = null;

    private int baseIndexFrom = 0;
    private int baseIndexSize = 10;

    private int point = 1;
    private int filedPoint = -1;

    public void computeNowSearchHits(SearchResponse searchResponse){

        List<Map<String, Object>> searchHitsList = Arrays.stream(searchResponse.getHits().getHits())
            .map(hit->{
                String index = hit.getIndex();
                String type = hit.getType();
                Map<String, Object> newSource = new HashMap<>();
                if(hit.getSource() != null && !hit.getSource().isEmpty()) {
                    hit.getSource().entrySet().stream().forEach(entry->{
                        newSource.put(String.join(".", index, type, entry.getKey()), entry.getValue());
                    });
                }
                if(hit.getFields() != null && !hit.getFields().isEmpty()) {
                    hit.getFields().entrySet().stream().forEach(entry->{
                        newSource.put(String.join(".", index, type, entry.getKey()), entry.getValue().getValue());
                    });
                }
                return newSource;
            })
            .collect(Collectors.toList());

        JoinFields filed;
        if(filedPoint==-1){
            filed=JoinFields.EMPTY;
            filedPoint++;
        }else{
            filed=joinFields.get(filedPoint++);
        }
        nowSearchHits = JoinQueryHelper.InnerJoin(nowSearchHits, searchHitsList, filed);
    }

    public List<Map<String, Object>> nowSearchHits(){
        return this.nowSearchHits;
    }

    public SearchRequest getFirst(){
        JoinQueryInfo info = joinQueryInfos.get(0);
        return wrapperBaseIndexSearchRequest(info.getIndex(), info.getType(), info.getContent());
    }

    public SearchRequest getNext(SearchResponse searchResponse) {
        computeNowSearchHits(searchResponse);
        if(point==joinQueryInfos.size()){
            return null;
        }
        JoinFields fields = joinFields.get(point - 1);
        List<String> responseValueList = nowSearchHits.stream()
            .filter(sourceMap->sourceMap.containsKey(fields.leftWithIndexAndType()))
            .map(source->String.valueOf(source.get(fields.leftWithIndexAndType())))
            .collect(Collectors.toList());

        JoinQueryInfo info = joinQueryInfos.get(point++);

        return wrapperExtraIndexSearchRequest(info.getIndex(), info.getType(), info.getContent(), fields.right(), responseValueList);

    }

    public JoinQueryRequest() {

    }

    public JoinQueryRequest addParserInfo(NamedXContentRegistry requestXContentRegistry,
                                          SearchRequestParsers searchRequestParsers,
                                          ParseFieldMatcher parseFieldMatcher){
        this.requestXContentRegistry = requestXContentRegistry;
        this.searchRequestParsers = searchRequestParsers;
        this.parseFieldMatcher = parseFieldMatcher;
        return this;
    }

    public JoinQueryRequest setBaseIndexFrom(int from){
        this.baseIndexFrom = from;
        return this;
    }

    public JoinQueryRequest setBaseIndexSize(int size){
        this.baseIndexSize = size;
        return this;
    }


    public List<JoinQueryInfo> joinQueryInfos() {
        return joinQueryInfos;
    }

    private SearchRequest wrapperBaseIndexSearchRequest(String index, String type, byte[] content){
        if(content==null){
            return null;
        }
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.types(type);
        try {
            XContentParser parser = XContentFactory.xContent(content).createParser(requestXContentRegistry, content);
            QueryParseContext queryParseContext = new QueryParseContext(parser, parseFieldMatcher);

            SearchSourceBuilder sourceBuilder = SearchSourceBuilder.
                fromXContent(queryParseContext, searchRequestParsers.aggParsers,
                    searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);

            sourceBuilder.size(baseIndexSize);
            sourceBuilder.from(baseIndexFrom);
            searchRequest.source(sourceBuilder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return searchRequest;
    }

    private SearchRequest wrapperExtraIndexSearchRequest(String index, String type, byte[] content, String fieldName, List<?> valueList){
        if(content==null){
            return null;
        }
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.types(type);
        try{
            XContentParser parser = XContentFactory.xContent(content).createParser(requestXContentRegistry, content);
            QueryParseContext queryParseContext = new QueryParseContext(parser, parseFieldMatcher);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

            SearchSourceBuilder sourceBuilder = SearchSourceBuilder.
                fromXContent(queryParseContext, searchRequestParsers.aggParsers,
                    searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);

            TermsQueryBuilder injectQueryBuilder = new TermsQueryBuilder(fieldName, valueList);
            boolQueryBuilder.must(sourceBuilder.query());
            boolQueryBuilder.filter(injectQueryBuilder);
            sourceBuilder.query(boolQueryBuilder);
            searchRequest.source(sourceBuilder);
        } catch(IOException e) {
            e.printStackTrace();
        }
        return searchRequest;
    }

    public JoinQueryRequest add(JoinQueryInfo request) {
        this.joinQueryInfos.add(request);
        return this;
    }

    public JoinQueryRequest addJoinField(JoinFields field) {
        this.joinFields.add(field);
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        //todo : validate
//        ActionRequestValidationException validationException = null;
//        if (nowSearchRequest == null) {
//            validationException = addValidationError("no request", validationException);
//        }
//        ActionRequestValidationException ex = nowSearchRequest.validate();
//        if (ex != null) {
//            if (validationException == null) {
//                validationException = new ActionRequestValidationException();
//            }
//            validationException.addValidationErrors(ex.validationErrors());
//        }
        return null;
    }
}
