package com.happy3w.persistence.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.happy3w.persistence.core.assistant.IDbAssistant;
import com.happy3w.persistence.core.assistant.QueryOptions;
import com.happy3w.persistence.core.filter.IFilter;
import com.happy3w.persistence.es.impl.DefaultEsIndexAssistant;
import com.happy3w.persistence.es.model.EsConnectConfig;
import com.happy3w.persistence.es.translator.EsFilterTranslator;
import com.happy3w.toolkits.iterator.EasyIterator;
import com.happy3w.toolkits.message.MessageRecorderException;
import com.happy3w.toolkits.reflect.FieldAccessor;
import com.happy3w.toolkits.utils.MapBuilder;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.UnknownHostException;
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

    // TODO: 需要配置哪些Index可以自动创建
    private Map<Class, DataTypeInfo> dataTypeInfoMap = new HashMap<>();

    public EsAssistant(TransportClient client) {
        this.client = client;
    }

    public void initSchema(Class dataType) {
        getDataTypeInfo(dataType);
    }

    public DataTypeInfo getDataTypeInfo(Class dataType) {
        return dataTypeInfoMap.computeIfAbsent(dataType,
                type -> createDataTypeInfo(dataType));
    }

    private DataTypeInfo createDataTypeInfo(Class dataType) {
        String indexName = indexAssistant.getIndexName(dataType);
        String typeName = indexName;

        IndicesExistsResponse indexExistResponse = client.admin().indices().exists(new IndicesExistsRequest().indices(indexName)).actionGet();
        if (!indexExistResponse.isExists()) {
            createIndex(indexName);
        }

        return new DataTypeInfo(dataType, indexName, typeName, FieldAccessor.from("id", dataType));
    }

    private void createIndex(String indexName) {
        Map mapping = MapBuilder.of("dynamic_templates", JSON.parseArray("[{\"strings\":{\"match_mapping_type\": \"string\",\"mapping\": {\"type\": \"keyword\"}}}]")).build();
//                MapBuilder.of("properties", indexAssistant.createMappingProperties(dataType)).build()
//        ).build();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest()
                .index(indexName)
                .mapping(indexName, mapping);
        client.admin().indices().create(createIndexRequest).actionGet();
    }

    @Override
    public <T> T saveData(T data) {
        ObjContext<T> context = (ObjContext<T>) createObjContext(data.getClass());
        IndexRequest indexRequest = createIndexRequest(data, context);
        IndexResponse response = client.index(indexRequest).actionGet();
        context.setId(data, response.getId());
        return data;
    }

    @Override
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
        DataTypeInfo dataTypeInfo = context.getDataTypeInfo();
        return new IndexRequest()
                .index(dataTypeInfo.getIndexName())
                .type(context.getType())
                .id((String) dataTypeInfo.getIdAccessor().getValue(data))
                .source(JSON.toJSONString(data,
                        SerializerFeature.DisableCircularReferenceDetect), XContentType.JSON);
    }

    private <T> ObjContext<T> createObjContext(Class<T> dataType) {
        DataTypeInfo typeInfo = getDataTypeInfo(dataType);
        String indexName = indexAssistant.getIndexName(dataType);
        String type = dataType.getSimpleName().toLowerCase();
        return new ObjContext<>(dataType, new String[]{indexName}, type, typeInfo);
    }

    @Override
    public <T> Stream<T> queryStream(Class<T> dataType, List<IFilter> filters, QueryOptions options) {
        ObjContext<T> context = createObjContext(dataType);

        SearchRequest request = new SearchRequest(context.getIndexNames())
                .searchType(SearchType.DEFAULT);
        SearchSourceBuilder requestBuilder = new SearchSourceBuilder()
                .postFilter(filterTranslator.translate(filters));
        request.source(requestBuilder)
                .scroll(TimeValue.MINUS_ONE);

        SearchResponse response = client.search(request).actionGet();
        ScrollCloseableIterator<T> it = new ScrollCloseableIterator<>(
                response,
                context,
                this,
                TimeValue.MINUS_ONE);
        return it.stream()
                .map(EsDocWrapper::getSource);
    }

    public <T> T deleteById(Class<T> dataType, String id) {
        ObjContext<T> context = createObjContext(dataType);
        DeleteRequest request = new DeleteRequest(context.getDataTypeInfo().getIndexName())
                .id(id);
        DeleteResponse response = client.delete(request).actionGet();
        return null;
    }

    public <T> long deleteByFilter(Class<T> dataType, List<IFilter> filters) {
        ObjContext<T> context = createObjContext(dataType);

        filterTranslator.translate(filters);

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(filterTranslator.translate(filters))
                .source(context.getDataTypeInfo().getIndexName())
                .get();

        return response.getDeleted();
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

    public static EsAssistant from(EsConnectConfig config) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", config.getClusterName())
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        for (TransportAddress addr : config.allTransportAddress()) {
            client.addTransportAddress(addr);
        }

        return new EsAssistant(client);
    }
}
