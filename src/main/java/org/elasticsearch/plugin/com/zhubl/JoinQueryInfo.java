package org.elasticsearch.plugin.com.zhubl;

public class JoinQueryInfo {
    private byte[] content ;
    private String index;
    private String type;

    public byte[] getContent(){
        return content;
    }

    public String getIndex(){
        return index;
    }

    public String getType(){
        return type;
    }

    public JoinQueryInfo(String index, String type, byte[] content){
        this.index = index;
        this.type = type;
        this.content = content;
    }
}
