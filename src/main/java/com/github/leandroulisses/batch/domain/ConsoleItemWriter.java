package com.github.leandroulisses.batch.domain;

import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
class ConsoleItemWriter implements ItemWriter<CsvMessage> {

    @Override
    public void write(List<? extends CsvMessage> list) {
        list.forEach(System.out::println);
    }

}
