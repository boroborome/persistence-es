package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.impl.StringLikeInFilter;
import com.happy3w.toolkits.utils.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;

import java.util.ArrayList;
import java.util.List;

public class StringLikeInTranslator implements IFilterTranslator<StringLikeInFilter> {
    @Override
    public Class<StringLikeInFilter> getFilterType() {
        return StringLikeInFilter.class;
    }

    @Override
    public void translate(StringLikeInFilter filter, List<QueryBuilder> queryBuilders, ITranslateAssistant translateAssistant) {
        List<WildcardQueryBuilder> builders = new ArrayList<>();
        for (String likeItem : filter.getRefs()) {
            if (StringUtils.hasText(likeItem)) {
                builders.add(QueryBuilders.wildcardQuery(filter.getField(), "*" + likeItem + "*"));
            }
        }
        if (builders.size() == 1) {
            queryBuilders.add(builders.get(0));
        } else if (builders.size() > 1) {
            BoolQueryBuilder combineBuilder = QueryBuilders.boolQuery();
            for (QueryBuilder builder : builders) {
                combineBuilder.should(builder);
            }
            queryBuilders.add(combineBuilder);
        }
    }
}
