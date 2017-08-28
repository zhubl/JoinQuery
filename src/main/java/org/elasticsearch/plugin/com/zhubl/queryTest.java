package org.elasticsearch.plugin.com.zhubl;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;


/**
 * Created by mrlion on 2017/8/17.
 */
public class queryTest {
    public static void main(String[] args){
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("student_course");
        searchRequest.types("all-type");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    }
}
