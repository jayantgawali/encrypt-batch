package com.wipro.batch.validate;

import com.wipro.batch.job.contants.JobContants;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

import java.io.File;

public class BatchJobParametersValidator implements JobParametersValidator {

    @Override
    public void validate(JobParameters jobParameters) throws JobParametersInvalidException {
        String file = jobParameters.getString(JobContants.SOURCE_FILE_PARAM);
        if(!(new File(file)).exists()) {
            throw new JobParametersInvalidException("Source File " + file + " does not exist");
        }
        String nofOfThreads = jobParameters.getString(JobContants.NO_OF_THREADS_PARAM);
        try {
            int threads = Integer.parseInt(nofOfThreads);
            if (threads < 1 || threads > 100) {
                throw new JobParametersInvalidException("No of Threads must be between 1 and 100");
            }
        } catch (Exception e) {
            throw new JobParametersInvalidException("Invalid no Of Threads provided: " + nofOfThreads);
        }
    }
}
