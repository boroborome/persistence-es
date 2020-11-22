package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringInFilter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class StringInTranslator implements IFilterTranslator<StringInFilter> {
    @Override
    public Class<StringInFilter> getFilterType() {
        return StringInFilter.class;
    }

    @Override
    public void translate(StringInFilter filter, List<QueryBuilder> queryBuilders, ITranslateAssistant translateAssistant) {
        queryBuilders.add(QueryBuilders.termsQuery(filter.getField(), filter.getRefs()));
    }
}
