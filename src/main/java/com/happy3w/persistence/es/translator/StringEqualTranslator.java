package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringEqualFilter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class StringEqualTranslator implements IFilterTranslator<StringEqualFilter> {
    @Override
    public Class<StringEqualFilter> getFilterType() {
        return StringEqualFilter.class;
    }

    @Override
    public void translate(StringEqualFilter filter, List<QueryBuilder> queryBuilders, ITranslateAssistant translateAssistant) {
        queryBuilders.add(QueryBuilders.termQuery(filter.getField(), filter.getRef()));
    }
}
