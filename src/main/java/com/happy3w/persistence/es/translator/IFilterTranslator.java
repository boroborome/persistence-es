package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import org.elasticsearch.index.query.QueryBuilder;

public interface IFilterTranslator<FT extends IFilter> {
    /**
     * 这个Translator支持的Filter类型
     * @return filter类型
     */
    Class<FT> getFilterType();

    /**
     * 将filter中内容转换为正向的QueryBuilder。转换的时候忽略属性filter.isPositive
     * @param filter 需要转换的filter
     * @param translateAssistant 当前assistant，用于辅助转换复杂的filter
     * @return 转换后的QueryBuilder
     */
    QueryBuilder translatePositive(FT filter, ITranslateAssistant translateAssistant);
}
