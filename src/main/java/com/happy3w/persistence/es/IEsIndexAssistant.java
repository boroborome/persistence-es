package com.happy3w.persistence.es;

import java.util.Map;

public interface IEsIndexAssistant {
    String getIndexName(Class docType);

    Map<String, Object> createMappingProperties(Class dataType);
}
