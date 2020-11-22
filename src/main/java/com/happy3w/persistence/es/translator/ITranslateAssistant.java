package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import org.elasticsearch.index.query.QueryBuilder;

public interface ITranslateAssistant {
    /**
     * 将filter转换为正向的queryBuilder
     * @param filter 过滤条件
     * @return es query builder.
     */
    QueryBuilder translatePositive(IFilter filter);
}
