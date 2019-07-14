package com.wipro.batch.job;

import com.wipro.batch.job.contants.JobContants;
import com.wipro.batch.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);


    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED!");
            cleanupFiles(jobExecution.getJobParameters().getString(JobContants.SOURCE_FILE_PARAM));
        }
    }

    private void cleanupFiles(String sourceFile) {
        log.info("Cleaning up the unwanted files.");
        File srcFile = new File(sourceFile);
        final File folder = srcFile.getParentFile();
        final File[] files = folder.listFiles( (dir, name) ->
                 (name.contains( srcFile.getName()) && name.startsWith(srcFile.getName() + "_")
                         || name.contains(Utils.OUTPUT_FILE_SUFFIX + "_"))
        );
        for ( final File file : files ) {
            if ( !file.delete() ) {
               log.error( "Can't remove " + file.getAbsolutePath() );
            }
        }
    }
}