package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringLikeInFilter;
import com.happy3w.toolkits.utils.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class StringLikeInTranslator implements IFilterTranslator<StringLikeInFilter> {
    @Override
    public Class<StringLikeInFilter> getFilterType() {
        return StringLikeInFilter.class;
    }

    @Override
    public QueryBuilder translatePositive(StringLikeInFilter filter, ITranslateAssistant translateAssistant) {
        QueryBuilderCombiner combiner = QueryBuilderCombiner.orCombiner();
        for (String likeItem : filter.getRefs()) {
            if (StringUtils.hasText(likeItem)) {
                combiner.append(QueryBuilders.wildcardQuery(filter.getField(), "*" + likeItem + "*"), true);
            }
        }
        return combiner.getCombinedResult();
    }
}
