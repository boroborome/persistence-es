package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import com.happy3w.persistence.core.filter.impl.CombineFilter;
import org.elasticsearch.index.query.QueryBuilder;

public class CombineTranslator implements IFilterTranslator<CombineFilter> {
    @Override
    public Class<CombineFilter> getFilterType() {
        return CombineFilter.class;
    }

    @Override
    public QueryBuilder translatePositive(CombineFilter filter, ITranslateAssistant translateAssistant) {
        QueryBuilderCombiner combiner = createCombinerByOps(filter.getOperator());
        for (IFilter innerFilter : filter.getInnerFilters()) {
            QueryBuilder innerBuilder = translateAssistant.translatePositive(innerFilter);
            combiner.append(innerBuilder, innerFilter.isPositive());
        }
        return combiner.getCombinedResult();
    }

    private QueryBuilderCombiner createCombinerByOps(String operator) {
        if (CombineFilter.Ops.And.equals(operator)) {
            return QueryBuilderCombiner.andCombiner();
        } else if (CombineFilter.Ops.Or.equals(operator)) {
            return QueryBuilderCombiner.orCombiner();
        } else {
            throw new UnsupportedOperationException("Operator '" + operator + "' is not supported in CombineTranslator.");
        }
    }
}
