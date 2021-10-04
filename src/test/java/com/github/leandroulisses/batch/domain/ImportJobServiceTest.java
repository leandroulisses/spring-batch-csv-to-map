package com.github.leandroulisses.batch.domain;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ImportJobServiceTest {

    @Autowired
    private ImportJobService importJobService;

    @Value("classpath:base.csv")
    private Resource resource;

    @Test
    void should_execute_job_with_success() throws IOException {
        JobExecution jobExecution = importJobService.execute(resource.getFile().getAbsolutePath());
        assertEquals(jobExecution.getStatus(), BatchStatus.COMPLETED);
    }

}