package com.github.leandroulisses.batch.domain;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.util.Map;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

    private static final int CHUNK_SIZE = 500;
    private final ConsoleItemWriter itemWriter;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public BatchConfiguration(ConsoleItemWriter itemWriter, JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory) {
        this.itemWriter = itemWriter;
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Map<String, String>> readCsvStep(@Value("#{jobParameters['fileLocation']}") String fileName) {

        return new CsvItemReaderBuilder<Map<String, String>>()
                .name("readCsvStep")
                .resource(new FileSystemResource(fileName))
                .encoding("UTF-8")
                .linesToSkip(1)
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<Map<String, String>, CsvMessage> itemProcessor(){
        return item -> {
            CsvMessage message = new CsvMessage();
            message.setType(item.get("type"));
            message.setValue(item.get("value"));
            return message;
        };
    }

    @Bean
    public Step processBase(FlatFileItemReader<Map<String, String>> readCsvStep,
                            ItemProcessor<Map<String, String>, CsvMessage> itemProcessor) {
        return stepBuilderFactory.get("processCSV").<Map<String, String>, CsvMessage>chunk(CHUNK_SIZE)
                .reader(readCsvStep)
                .processor(itemProcessor)
                .faultTolerant()
                .writer(itemWriter)
                .build();
    }

    @Bean
    public Job readCSVFileJob(Step processBase) {
        return jobBuilderFactory
                .get("readCSVFileJob")
                .incrementer(new RunIdIncrementer())
                .start(processBase)
                .build();
    }

}
