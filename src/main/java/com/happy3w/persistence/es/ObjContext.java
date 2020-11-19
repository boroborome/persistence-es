package com.happy3w.persistence.es;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
public class ObjContext<T> {
    private Class<T> dataType;
    // current index name used
    private String[] indexNames;
    private String type;
    private DataTypeInfo dataTypeInfo;

    public String getId(T data) {
        return (String) dataTypeInfo.getIdAccessor().getValue(data);
    }

    public void setId(T data, String id) {
        dataTypeInfo.getIdAccessor().setValue(data, id);
    }

    public T parseData(String json, String id) {
        T value = JSON.parseObject(json, dataType);
        if (id != null) {
            dataTypeInfo.getIdAccessor().setValue(value, id);
        }
        return value;
    }
}
