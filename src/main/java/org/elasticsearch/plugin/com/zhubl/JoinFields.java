package org.elasticsearch.plugin.com.zhubl;


import org.elasticsearch.common.collect.Tuple;

public class JoinFields {
    private Fields leftField;
    private Fields rightField;

    public static JoinFields EMPTY = new JoinFields(null, null);

    public boolean empty(){
        return leftField == null && rightField == null;
    }

    public JoinFields(Fields leftField, Fields rightField) {
        this.leftField = leftField;
        this.rightField = rightField;
    }

    public Tuple<Fields, Fields> getFields() {
        return new Tuple<>(leftField, rightField);
    }

    public String left() {
        return leftField.field;
    }

    public String right() {
        return rightField.field;
    }

    public String leftWithIndexAndType(){
        return leftField.toString();
    }

    public String rightWithIndexAndType(){
        return rightField.toString();
    }

    @Override
    public String toString() {
        return "JoinFiled[" + leftField + "," + rightField + "]";
    }

    public static class Fields{
        private String index;
        private String type;
        private String field;

        public Fields(String index, String type, String field){
            this.index = index;
            this.type = type;
            this.field = field;
        }

        public String toString(){
            return String.join(".", index, type, field);
        }
    }
}

