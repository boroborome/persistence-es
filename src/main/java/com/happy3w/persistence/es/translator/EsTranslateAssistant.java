package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import lombok.Getter;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class EsTranslateAssistant implements ITranslateAssistant {
    public static final EsTranslateAssistant INSTANCE = new EsTranslateAssistant();
    static {
        INSTANCE.regist(new StringEqualTranslator());
        INSTANCE.regist(new StringInTranslator());
        INSTANCE.regist(new StringLikeTranslator());
        INSTANCE.regist(new StringLikeInTranslator());
        INSTANCE.regist(new CombineTranslator());
    }

    private Map<Class<? extends IFilter>, IFilterTranslator> translatorMap = new HashMap<>();

    public void regist(IFilterTranslator translator) {
        translatorMap.put(translator.getFilterType(), translator);
    }

    public QueryBuilder translate(List<IFilter> filters) {
        QueryBuilderCombiner combiner = QueryBuilderCombiner.andCombiner();
        for (IFilter filter : filters) {
            QueryBuilder builder = translatePositive(filter);
            combiner.append(builder, filter.isPositive());
        }
        QueryBuilder finalBuilder = combiner.getCombinedResult();
        return finalBuilder == null ? QueryBuilders.matchAllQuery() : finalBuilder;
    }

    @Override
    public QueryBuilder translatePositive(IFilter filter) {
        IFilterTranslator translator = translatorMap.get(filter.getClass());
        if (translator == null) {
            throw new UnsupportedOperationException("Unsupported filter type:" + filter.getClass());
        }
        return translator.translatePositive(filter, this);
    }
}
