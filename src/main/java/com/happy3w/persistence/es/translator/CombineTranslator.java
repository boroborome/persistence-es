package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import com.happy3w.persistence.core.filter.impl.CombineFilter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CombineTranslator implements IFilterTranslator<CombineFilter> {
    @Override
    public Class<CombineFilter> getFilterType() {
        return CombineFilter.class;
    }

    @Override
    public void translate(CombineFilter filter, List<QueryBuilder> queryBuilders, ITranslateAssistant translateAssistant) {
        List<QueryBuilder> subBuilders = new ArrayList<>();
        for (IFilter innerFilter : filter.getInnerFilters()) {
            translateAssistant.translate(innerFilter, subBuilders);
        }
        if (subBuilders.size() == 1) {
            queryBuilders.add(subBuilders.get(0));
        } else if (subBuilders.size() > 1) {
            BoolQueryBuilder combineBuilder = QueryBuilders.boolQuery();
            Function<QueryBuilder, BoolQueryBuilder> consumer = getConsumerByOps(filter.getOperator(), combineBuilder);
            for (QueryBuilder builder : subBuilders) {
                consumer.apply(builder);
            }
            queryBuilders.add(combineBuilder);
        }
    }

    private Function<QueryBuilder, BoolQueryBuilder> getConsumerByOps(String operator, BoolQueryBuilder combineBuilder) {
        if (CombineFilter.Ops.And.equals(operator)) {
            return combineBuilder::must;
        } else if (CombineFilter.Ops.Or.equals(operator)) {
            return combineBuilder::should;
        } else {
            throw new UnsupportedOperationException("Operator '" + operator + "' is not supported in CombineTranslator.");
        }
    }
}
