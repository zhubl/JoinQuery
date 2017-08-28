package org.elasticsearch.plugin.com.zhubl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class JoinQueryHelper {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static JoinSearchHits GetJoinSearchHits(List<Map<String, Object>> hits) {
        if(hits==null) {
            return JoinSearchHits.empty();
        }
        List<SearchHit> searchHitRequest = hits.stream().map(hit->{
            InternalSearchHit internalSearchHit = new InternalSearchHit(0);
            try {
                internalSearchHit.sourceRef(new BytesArray(mapper.writeValueAsBytes(hit)));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return internalSearchHit;
        }).collect(Collectors.toList());
        int numHits = searchHitRequest.size();
        return new JoinSearchHits(searchHitRequest.toArray(new SearchHit[numHits]), numHits);
    }

    static List<Map<String, Object>> LeftJoin(List<Map<String, Object>> leftSource,
                                              List<Map<String, Object>> rightSource,
                                              JoinFields fields) {
        if(leftSource == null || leftSource.size() == 0){
            return null;
        }
        if(rightSource == null || rightSource.size() == 0 ){
            return leftSource;
        }

        Map<Object, List<Map<String, Object>>> leftMap = leftSource.stream()
            .filter(map->map.containsKey(fields.leftWithIndexAndType()))
            .collect(Collectors.groupingBy(hit->hit.get(fields.leftWithIndexAndType())));

        if(leftMap.isEmpty()){
            return null;
        }

        Map<Object, List<Map<String, Object>>> rightMap = rightSource.stream()
            .filter(map->map.containsKey(fields.rightWithIndexAndType()))
            .collect(Collectors.groupingBy(hit->hit.get(fields.rightWithIndexAndType())));

        rightMap.entrySet().stream().forEach(entry->{
            leftMap.computeIfPresent(entry.getKey(), (key, value)->{
                List<Map<String, Object>> newHitList = new ArrayList<>();
                value.forEach(lHit->{
                    entry.getValue().forEach(rHit->{
                        Map<String, Object> crossHit = new HashMap<>();
                        crossHit.putAll(rHit);
                        crossHit.putAll(lHit);
                        newHitList.add(crossHit);
                    });
                });
                return newHitList;
            });
        });
        return leftMap.values().stream().flatMap(eachList -> eachList.stream()).collect(Collectors.toList());
    }

    static List<Map<String, Object>> InnerJoin(List<Map<String, Object>> leftSource,
                                              List<Map<String, Object>> rightSource,
                                              JoinFields fields) {
        if(leftSource == null || leftSource.size() == 0){
            return rightSource;
        }
        if(rightSource == null || rightSource.size() == 0 ){
            return leftSource;
        }

        Map<Object, List<Map<String, Object>>> leftMap = leftSource.stream()
            .filter(map->map.containsKey(fields.leftWithIndexAndType()))
            .collect(Collectors.groupingBy(hit->hit.get(fields.leftWithIndexAndType())));

        Map<Object, List<Map<String, Object>>> rightMap = rightSource.stream()
            .filter(map->map.containsKey(fields.rightWithIndexAndType()))
            .collect(Collectors.groupingBy(hit->hit.get(fields.rightWithIndexAndType())));

        Map<Object, List<Map<String, Object>>> newMap = new HashMap<>();

        leftMap.entrySet().stream().forEach(entry->{
            if(rightMap.containsKey(entry.getKey())){
                List<Map<String, Object>> newHitList = new ArrayList<>();
                entry.getValue().stream().forEach(lHit->
                    rightMap.get(entry.getKey()).stream().forEach(rHit-> {
                        Map<String, Object> crossHit = new HashMap<>();
                        crossHit.putAll(rHit);
                        crossHit.putAll(lHit);
                        newHitList.add(crossHit);
                    })
                );
                newMap.put(entry.getKey(), newHitList);
            }
        });
        return newMap.values().stream().flatMap(eachList -> eachList.stream()).collect(Collectors.toList());
    }
}
