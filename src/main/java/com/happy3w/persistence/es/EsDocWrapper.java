package com.happy3w.persistence.es;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EsDocWrapper<T> {
    private T source;
    private Map<String, Object> extendValues;
    private Long version;

    public EsDocWrapper(T source, Long version) {
        this.source = source;
        this.version = version;
    }

    public void increaseVersion() {
        if (version == null) {
            return;
        }
        version++;
    }
}
