package com.happy3w.persistence.es.impl;

import com.happy3w.persistence.es.IEsIndexAssistant;

public class DefaultEsIndexAssistant implements IEsIndexAssistant {

    @Override
    public String getIndexName(Class docType) {
        return docType.getSimpleName().toLowerCase();
    }
}
