package com.wipro.batch.job;

import com.wipro.batch.AppConfig;
import com.wipro.batch.job.contants.JobContants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes= AppConfig.class)
@SpringBatchTest
@TestPropertySource(
        locations = "classpath:test.properties")
public class FileEncryptJobBatchConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test(expected = JobParametersInvalidException.class)
    public void testJobNoJobParams() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
    }

    @Test(expected = JobParametersInvalidException.class)
    public void testJobInvalidSourceFileParams() throws Exception {
        String pathname = "temp/temp-invalid-input.txt";
        Map<String, String> params = new HashMap<>();
        params.put(JobContants.SOURCE_FILE_PARAM, pathname);
        params.put(JobContants.NO_OF_THREADS_PARAM, "3");


        Properties property = new Properties();
        for (Map.Entry<String, String> set : params.entrySet()) {
            property.put(set.getKey(), set.getValue());
        }
        JobParameters jobParameters = new DefaultJobParametersConverter().getJobParameters(property);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
    }

    @Test(expected = JobParametersInvalidException.class)
    public void testJobInvalidThreadsParams() throws Exception {
        String pathname = "temp/temp-valid-input.txt";
        File file = createInputFile(pathname);
        Map<String, String> params = new HashMap<>();
        params.put(JobContants.SOURCE_FILE_PARAM, pathname);
        params.put(JobContants.NO_OF_THREADS_PARAM, "0");


        Properties property = new Properties();
        for (Map.Entry<String, String> set : params.entrySet()) {
            property.put(set.getKey(), set.getValue());
        }
        JobParameters jobParameters = new DefaultJobParametersConverter().getJobParameters(property);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
    }

    @Test
    public void testJob() throws Exception {
        String pathname = "temp/temp-input.txt";
        File file = createInputFile(pathname);
        Map<String, String> params = new HashMap<>();
        params.put(JobContants.SOURCE_FILE_PARAM, pathname);
        params.put(JobContants.NO_OF_THREADS_PARAM, "3");


        Properties property = new Properties();
        for (Map.Entry<String, String> set : params.entrySet()) {
            property.put(set.getKey(), set.getValue());
        }
        JobParameters jobParameters = new DefaultJobParametersConverter().getJobParameters(property);

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);


        Assert.assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        File resultFile = new File(pathname + "-result");
        Assert.assertTrue(resultFile.exists());
        Assert.assertEquals(Files.readAllLines(resultFile.toPath()).size(), 3);

        cleanupFiles(file, resultFile);
    }

    private void cleanupFiles(File file, File resultFile) {
        resultFile.delete();
        file.delete();
    }

    private File createInputFile(String pathname) throws Exception{
        String input = "Line1\nLine2\nLine3";

        new File("temp").mkdir();
        File file = new File(pathname);
        Files.write(file.toPath(), input.getBytes());
        return file;
    }

}
