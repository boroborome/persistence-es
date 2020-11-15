package com.happy3w.persistence.es;

import com.alibaba.fastjson.JSON;
import com.happy3w.persistence.core.filter.impl.StringEqualFilter;
import com.happy3w.persistence.es.model.EsConnectConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EsAssistantTest {

    @Test
    public void should_write_and_read_success() throws UnknownHostException {
        EsAssistant esAssistant = EsAssistant.from(new EsConnectConfig("127.0.0.1:9300", "docker-cluster"));
        esAssistant.saveData(new MyData("tom", "Tom", 22));
        esAssistant.saveData(new MyData("jerry", "Jerry", 20));
        esAssistant.flush(MyData.class);
        List<MyData> results = esAssistant.queryStream(MyData.class,
                Arrays.asList(new StringEqualFilter("name", "Tom")),
                null)
                .collect(Collectors.toList());
        Assert.assertEquals("[{\"age\":22,\"id\":\"tom\",\"name\":\"Tom\"}]",
                JSON.toJSONString(results));
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyData {
        private String id;
        private String name;
        private int age;
    }
}
