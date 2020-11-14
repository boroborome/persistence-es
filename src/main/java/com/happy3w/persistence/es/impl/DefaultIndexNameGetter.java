package com.happy3w.persistence.es.impl;

import com.happy3w.persistence.es.IIndexNameGetter;

public class DefaultIndexNameGetter implements IIndexNameGetter {

    @Override
    public String getIndexName(Class docType) {
        return docType.getSimpleName().toLowerCase();
    }
}
