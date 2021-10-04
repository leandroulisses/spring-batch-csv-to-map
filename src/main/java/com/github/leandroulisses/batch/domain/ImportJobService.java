package com.github.leandroulisses.batch.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.UUID;

@Service
public class ImportJobService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportJobService.class);
    private final JobLauncher jobLauncher;
    private final Job job;

    public ImportJobService(JobLauncher jobLauncher, Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
    }

    public JobExecution execute(String fileLocation) {
        try {
            Path path = Path.of(fileLocation);
            String executionId = UUID.nameUUIDFromBytes(path.toString().getBytes()).toString();
            LOG.info("Starting execution for file : {}", path);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString(JobParameterKey.IMPORT_ID.getValue(), executionId)
                    .addString(JobParameterKey.TEMP_FILE_LOCATION.getValue(), path.toString(), Boolean.FALSE)
                    .toJobParameters();

            return jobLauncher.run(job, jobParameters);
        } catch (Exception e) {
            String errorMessage = "Error on launching Job Process: ";
            LOG.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

}
