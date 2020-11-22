package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

public interface ITranslateAssistant {
    void translate(IFilter filter, List<QueryBuilder> queryBuilders);
}
