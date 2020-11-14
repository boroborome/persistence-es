package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

public interface IFilterTranslator<FT extends IFilter> {
    Class<FT> getFilterType();
    void translate(FT filter,  List<QueryBuilder> queryBuilders);
}
