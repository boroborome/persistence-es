package com.happy3w.persistence.es;

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
}
