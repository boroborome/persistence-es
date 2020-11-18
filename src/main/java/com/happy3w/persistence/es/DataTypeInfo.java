package com.happy3w.persistence.es;

import com.happy3w.toolkits.reflect.FieldAccessor;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DataTypeInfo {
    private Class dataType;
    // default index name
    private String indexName;
    private String type;
    private FieldAccessor idAccessor;
}
