package com.happy3w.persistence.es.translator;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.function.BiConsumer;

public class QueryBuilderCombiner {
    private QueryBuilder innerBuilder;
    private final BiConsumer<QueryBuilder, BoolQueryBuilder> positiveAppend;
    private final BiConsumer<QueryBuilder, BoolQueryBuilder> negativeAppend;

    private QueryBuilderCombiner(
            BiConsumer<QueryBuilder, BoolQueryBuilder> positiveAppend,
            BiConsumer<QueryBuilder, BoolQueryBuilder> negativeAppend) {
        this.positiveAppend = positiveAppend;
        this.negativeAppend = negativeAppend;
    }

    public void append(QueryBuilder builder, boolean positive) {
        if (innerBuilder == null) {
            if (positive) {
                innerBuilder = builder;
            } else {
                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                negativeAppend.accept(builder, boolQueryBuilder);
                innerBuilder = boolQueryBuilder;
            }
        } else {
            if (!(innerBuilder instanceof BoolQueryBuilder)) {
                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                positiveAppend.accept(innerBuilder, boolQueryBuilder);
                innerBuilder = boolQueryBuilder;
            }
            BoolQueryBuilder boolBuilder = (BoolQueryBuilder) innerBuilder;
            BiConsumer<QueryBuilder, BoolQueryBuilder> appender = positive ? positiveAppend : negativeAppend;
            appender.accept(builder, boolBuilder);
        }
    }

    public QueryBuilder getCombinedResult() {
        return innerBuilder;
    }

    public static QueryBuilderCombiner andCombiner() {
        return new QueryBuilderCombiner(QueryBuilderCombiner::andPositiveAppend, QueryBuilderCombiner::andNegativeAppend);
    }

    public static QueryBuilderCombiner orCombiner() {
        return new QueryBuilderCombiner(QueryBuilderCombiner::orPositiveAppend, QueryBuilderCombiner::orNegativeAppend);
    }

    private static void andPositiveAppend(QueryBuilder queryBuilder, BoolQueryBuilder boolQueryBuilder) {
        boolQueryBuilder.filter(queryBuilder);
    }

    private static void andNegativeAppend(QueryBuilder queryBuilder, BoolQueryBuilder boolQueryBuilder) {
        boolQueryBuilder.mustNot(queryBuilder);
    }
    private static void orPositiveAppend(QueryBuilder queryBuilder, BoolQueryBuilder boolQueryBuilder) {
        boolQueryBuilder.should(queryBuilder);
    }

    private static void orNegativeAppend(QueryBuilder queryBuilder, BoolQueryBuilder boolQueryBuilder) {
        boolQueryBuilder.should(new BoolQueryBuilder().mustNot(queryBuilder));
    }
}
