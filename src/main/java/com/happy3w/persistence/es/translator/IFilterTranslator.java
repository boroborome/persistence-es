package com.happy3w.persistence.es.translator;

import com.happy3w.persistence.core.filter.IFilter;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

public interface IFilterTranslator<FT extends IFilter> {
    /**
     * 这个Translator支持的Filter类型
     * @return filter类型
     */
    Class<FT> getFilterType();

    /**
     * 将filter内容填写到builder列表中
     * @param filter 需要翻译的filter
     * @param queryBuilders 翻译后结果
     * @param translateAssistant 当前assistant
     */
    void translate(FT filter,  List<QueryBuilder> queryBuilders, ITranslateAssistant translateAssistant);
}
