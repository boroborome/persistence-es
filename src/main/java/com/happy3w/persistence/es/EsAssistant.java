package com.happy3w.persistence.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.happy3w.persistence.core.assistant.IDbAssistant;
import com.happy3w.persistence.core.assistant.QueryOptions;
import com.happy3w.persistence.core.filter.IFilter;
import com.happy3w.persistence.es.impl.DefaultEsIndexAssistant;
import com.happy3w.persistence.es.translator.EsFilterTranslator;
import com.happy3w.toolkits.iterator.EasyIterator;
import com.happy3w.toolkits.message.MessageRecorderException;
import com.happy3w.toolkits.reflect.FieldAccessor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


public class EsAssistant implements IDbAssistant {
    @Getter
    private TransportClient client;

    @Getter
    @Setter
    private EsFilterTranslator filterTranslator = EsFilterTranslator.INSTANCE;

    @Getter
    @Setter
    private IEsIndexAssistant indexAssistant = new DefaultEsIndexAssistant();

    private Map<Class, FieldAccessor> idAccessorMap = new HashMap<>();

    public EsAssistant(TransportClient client) {
        this.client = client;
    }

    public FieldAccessor getIdAccessor(Class dataType) {
        return idAccessorMap.computeIfAbsent(dataType, key -> FieldAccessor.from("id", key));
    }

    public <T> void saveDoc(T data) {
        ObjContext<T> context = (ObjContext<T>) createObjContext(data.getClass());
        IndexRequest indexRequest = createIndexRequest(data, context);
        client.index(indexRequest).actionGet();
    }

    public <T> void saveStream(Stream<T> dataStream) {
        EasyIterator<T> it = EasyIterator.from(dataStream);
        if (it.hasNext()) {
            T firstDoc = it.next();
            ObjContext<T> context = (ObjContext<T>) createObjContext(firstDoc.getClass());

            EasyIterator.of(firstDoc)
                    .concat(it)
                    .map(doc -> createIndexRequest(doc, context))
                    .split(100)
                    .forEach(requestList -> saveList(requestList, context));
        }
    }

    private <T> void saveList(List<IndexRequest> requestList, ObjContext<T> context) {
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest request : requestList) {
            bulkRequest.add(request);
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        checkError(response);
    }

    private void checkError(BulkResponse response) {
        if (response.hasFailures()) {
            throw new MessageRecorderException("Failed to save data:" + response.buildFailureMessage());
        }
        for (BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                throw new MessageRecorderException("Failed to save data:" + item.getFailureMessage());
            }
        }
    }

    private <T> IndexRequest createIndexRequest(T data, ObjContext<T> context) {
        return new IndexRequest()
                .index(context.getIndexName())
                .type(context.getType())
                .id((String) getIdAccessor(data.getClass()).getValue(data))
                .source(JSON.toJSONString(data,
                        SerializerFeature.DisableCircularReferenceDetect), XContentType.JSON);
    }

    public <T> ObjContext<T> createObjContext(Class<T> dataType) {
        String indexName = indexAssistant.getIndexName(dataType);
        String type = dataType.getSimpleName().toLowerCase();
        return new ObjContext<>(dataType, indexName, type);
    }

    @Override
    public <T> Stream<T> queryStream(Class<T> dataType, List<IFilter> filters, QueryOptions options) {
        SearchRequest request = new SearchRequest(indexAssistant.getIndexName(dataType))
                .searchType(SearchType.DEFAULT);
        SearchSourceBuilder requestBuilder = new SearchSourceBuilder();
        filterTranslator.translate(filters, requestBuilder);
        request.source(requestBuilder)
                .scroll(TimeValue.MINUS_ONE);

        SearchResponse response = client.search(request).actionGet();
        ScrollCloseableIterator<T> it = new ScrollCloseableIterator<>(
                response,
                dataType,
                this,
                TimeValue.MINUS_ONE);
        return it.stream()
                .map(EsDocWrapper::getSource);
    }

    public void flush(Class dataType) {
        String indexName = indexAssistant.getIndexName(dataType);

        FlushRequest request = new FlushRequest(indexName)
                .force(true);
        FlushResponse response = client.admin().indices().flush(request)
                .actionGet();
        System.out.println(response.getTotalShards()+","+response.getSuccessfulShards()+","+response.getFailedShards());
    }

    public void clearScroll(String scrollId) {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        client.clearScroll(request).actionGet();
    }

    public SearchResponse scrollSearch(String scrollId, TimeValue scrollTimeout) {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(scrollTimeout);
        return client.searchScroll(scrollRequest).actionGet();
    }


    @Getter
    @AllArgsConstructor
    @Builder
    private static class ObjContext<T> {
        private Class<T> dataType;
        private String indexName;
        private String type;
    }
}
