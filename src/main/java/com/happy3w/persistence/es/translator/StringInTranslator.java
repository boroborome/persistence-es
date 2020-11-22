package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringInFilter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class StringInTranslator implements IFilterTranslator<StringInFilter> {
    @Override
    public Class<StringInFilter> getFilterType() {
        return StringInFilter.class;
    }

    @Override
    public QueryBuilder translatePositive(StringInFilter filter, ITranslateAssistant translateAssistant) {
        return QueryBuilders.termsQuery(filter.getField(), filter.getRefs());
    }
}
