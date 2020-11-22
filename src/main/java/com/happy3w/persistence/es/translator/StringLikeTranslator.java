package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringLikeFilter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class StringLikeTranslator implements IFilterTranslator<StringLikeFilter> {
    @Override
    public Class<StringLikeFilter> getFilterType() {
        return StringLikeFilter.class;
    }

    @Override
    public void translate(StringLikeFilter filter, List<QueryBuilder> queryBuilders) {
        queryBuilders.add(QueryBuilders.wildcardQuery(filter.getField(), filter.getRef()));
    }
}
