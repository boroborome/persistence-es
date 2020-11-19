package com.happy3w.persistence.es;

import com.alibaba.fastjson.JSON;
import com.happy3w.toolkits.iterator.CloseableIterator;
import com.happy3w.toolkits.iterator.NeedFindIterator;
import com.happy3w.toolkits.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ScrollCloseableIterator<T>
        extends NeedFindIterator<EsDocWrapper<T>>
        implements CloseableIterator<EsDocWrapper<T>> {
    private SearchResponse response;
    private ObjContext<T> context;
    private EsAssistant assistant;
    private TimeValue scrollTimeout;

    private Iterator<SearchHit> innerIt;

    public ScrollCloseableIterator(
            SearchResponse response,
            ObjContext<T> context,
            EsAssistant assistant,
            TimeValue scrollTimeout) {
        this.context = context;
        this.assistant = assistant;
        this.response = response;
        this.scrollTimeout = scrollTimeout == null ? TimeValue.timeValueMinutes(1L) : scrollTimeout;
        innerIt = response.getHits().iterator();
    }

    @Override
    public void close() {
        if (response != null && response.getScrollId() != null) {
            assistant.clearScroll(response.getScrollId());
        }
    }

    @Override
    public Optional<EsDocWrapper<T>> findNext() {
        if (!innerIt.hasNext()) {
            if (response != null && StringUtils.hasText(response.getScrollId())) {
                response = assistant.scrollSearch(response.getScrollId(), scrollTimeout);
                innerIt = response.getHits().iterator();
            }
        }

        if (innerIt.hasNext()) {
            EsDocWrapper<T> nextItem = createWrapper(innerIt.next());
            return Optional.ofNullable(nextItem);
        }
        return Optional.empty();
    }

    private EsDocWrapper<T> createWrapper(SearchHit hit) {
        EsDocWrapper<T> wrapper = new EsDocWrapper<>();
        wrapper.setSource(context.parseData(hit.getSourceAsString(), hit.getId()));
        wrapper.setVersion(hit.getVersion());
        collectFields(wrapper, hit.getFields());
        return wrapper;
    }

    private void collectFields(EsDocWrapper<T> wrapper, Map<String, DocumentField> fields) {
        if (fields == null || fields.isEmpty()) {
            return;
        }

        Map<String, Object> extendedValues = new HashMap<>();
        for (Map.Entry<String, DocumentField> fieldEntry : fields.entrySet()) {
            extendedValues.put(fieldEntry.getKey(), fieldEntry.getValue().getValue());
        }
        wrapper.setExtendValues(extendedValues);
    }
}
