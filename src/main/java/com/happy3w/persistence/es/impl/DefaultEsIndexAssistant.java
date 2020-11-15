package com.happy3w.persistence.es.impl;

import com.happy3w.persistence.es.IEsIndexAssistant;
import com.happy3w.toolkits.reflect.FieldAccessor;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DefaultEsIndexAssistant implements IEsIndexAssistant {
    public static Map<Class, String> DEFAULT_PROPERTY_TYPE_MAP = new HashMap<>();
    static {
        DEFAULT_PROPERTY_TYPE_MAP.put(String.class, "keyword");
        DEFAULT_PROPERTY_TYPE_MAP.put(Long.class, "long");
        DEFAULT_PROPERTY_TYPE_MAP.put(long.class, "long");
        DEFAULT_PROPERTY_TYPE_MAP.put(Integer.class, "integer");
        DEFAULT_PROPERTY_TYPE_MAP.put(int.class, "integer");
        DEFAULT_PROPERTY_TYPE_MAP.put(Double.class, "double");
        DEFAULT_PROPERTY_TYPE_MAP.put(double.class, "double");
        DEFAULT_PROPERTY_TYPE_MAP.put(Float.class, "float");
        DEFAULT_PROPERTY_TYPE_MAP.put(float.class, "float");
        DEFAULT_PROPERTY_TYPE_MAP.put(Character.class, "short");
        DEFAULT_PROPERTY_TYPE_MAP.put(char.class, "short");
        DEFAULT_PROPERTY_TYPE_MAP.put(Byte.class, "byte");
        DEFAULT_PROPERTY_TYPE_MAP.put(byte.class, "byte");
        DEFAULT_PROPERTY_TYPE_MAP.put(Boolean.class, "boolean");
        DEFAULT_PROPERTY_TYPE_MAP.put(boolean.class, "boolean");
        DEFAULT_PROPERTY_TYPE_MAP.put(Date.class, "date");
        DEFAULT_PROPERTY_TYPE_MAP.put(Timestamp.class, "date");
    }

    @Getter
    @Setter
    private Map<Class, String> propertyTypeMap = new HashMap<>(DEFAULT_PROPERTY_TYPE_MAP);

    @Override
    public String getIndexName(Class docType) {
        return docType.getSimpleName().toLowerCase();
    }

    @Override
    public Map<String, Object> createMappingProperties(Class dataType) {
        Map<String, Object> properties = new HashMap<>();
        for (FieldAccessor fieldAccessor : FieldAccessor.allFieldAccessors(dataType)) {
            Class propertyType = fieldAccessor.getPropertyType();

            String mapType = propertyTypeMap.get(propertyType);
            if (mapType != null) {
                properties.put(fieldAccessor.getFieldName(), mapType);
            } else if (propertyType.isArray() || Collection.class.isAssignableFrom(propertyType)) {
                properties.put(fieldAccessor.getFieldName(), createListMapping(propertyType));
            } else if (Map.class.isAssignableFrom(propertyType)) {
                properties.put(fieldAccessor.getFieldName(), createMapMapping(propertyType));
            } else {
                properties.put(fieldAccessor.getFieldName(), createMappingProperties(propertyType));
            }
        }
        return properties;
    }

    private Object createMapMapping(Class propertyType) {
        throw new UnsupportedOperationException("Unsupported Mapping type:" + propertyType);
    }

    private Object createListMapping(Class propertyType) {
        throw new UnsupportedOperationException("Unsupported List type:" + propertyType);
    }
}
