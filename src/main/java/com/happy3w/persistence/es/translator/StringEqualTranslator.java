package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringEqualFilter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class StringEqualTranslator implements IFilterTranslator<StringEqualFilter> {
    @Override
    public Class<StringEqualFilter> getFilterType() {
        return StringEqualFilter.class;
    }

    @Override
    public QueryBuilder translatePositive(StringEqualFilter filter, ITranslateAssistant translateAssistant) {
        return QueryBuilders.termQuery(filter.getField(), filter.getRef());
    }
}
