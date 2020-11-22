package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import lombok.Getter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
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
        List<QueryBuilder> queryBuilders = new ArrayList<>();
        for (IFilter filter : filters) {
            IFilterTranslator translator = translatorMap.get(filter.getClass());
            if (translator == null) {
                throw new UnsupportedOperationException("Unsupported filter type:" + filter.getClass());
            }
            translator.translate(filter, queryBuilders, this);
        }

        if (queryBuilders.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        } else if (queryBuilders.size() == 1) {
            return queryBuilders.get(0);
        } else {
            BoolQueryBuilder combineBuilder = new BoolQueryBuilder();
            for (QueryBuilder queryBuilder : queryBuilders) {
                combineBuilder.must(queryBuilder);
            }
            return combineBuilder;
        }
    }

    @Override
    public void translate(IFilter filter, List<QueryBuilder> queryBuilders) {
        IFilterTranslator translator = translatorMap.get(filter.getClass());
        if (translator == null) {
            throw new UnsupportedOperationException("Unsupported filter type:" + filter.getClass());
        }
        translator.translate(filter, queryBuilders, this);
    }
}
