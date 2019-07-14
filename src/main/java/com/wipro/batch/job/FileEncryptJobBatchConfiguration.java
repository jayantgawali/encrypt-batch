package com.wipro.batch.job;

import com.wipro.batch.encrypt.EncryptionProcessor;
import com.wipro.batch.exception.UnExpectedBatchError;
import com.wipro.batch.util.Utils;
import com.wipro.batch.validate.BatchJobParametersValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import static com.wipro.batch.job.contants.JobContants.NO_OF_THREADS_PARAM;
import static com.wipro.batch.job.contants.JobContants.SOURCE_FILE_PARAM;

@Configuration
@EnableBatchProcessing
public class FileEncryptJobBatchConfiguration {

    private static final Logger log = LoggerFactory.getLogger(FileEncryptJobBatchConfiguration.class);

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    ResourcePatternResolver resourcePatternResolver;


    @Bean
    public EncryptionProcessor encryptProcessor(@Value("${encrypt.shift.characters}") String noOfChars) {
        return new EncryptionProcessor(noOfChars);
    }

    @Bean
    public Job fileEncryptJob(JobCompletionNotificationListener listener,
                              @Qualifier("splitFilesStep") Step splitFilesStep,
                              @Qualifier("encryptLines") Step encryptLines,
                              @Qualifier("mergeFiles") Step mergeFiles) {
        log.info("creating fileEncryptJob job");
        return jobBuilderFactory.get("fileEncryptJob")
                .validator(validator())
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(splitFilesStep)
                .next(encryptLines)
                .next(mergeFiles)
                .build();
    }

    @Bean
    public JobParametersValidator validator() {
        CompositeJobParametersValidator compositeJobParametersValidator = new CompositeJobParametersValidator();

        List<JobParametersValidator> validators = new ArrayList<>();
        validators.add(mandatoryValidator());
        validators.add(valueValidator());
        compositeJobParametersValidator.setValidators(validators);
        return compositeJobParametersValidator;
    }

    private JobParametersValidator valueValidator() {
        return new BatchJobParametersValidator();
    }

    private JobParametersValidator mandatoryValidator() {
        final DefaultJobParametersValidator parametersValidator = new DefaultJobParametersValidator();

        final String[] required = new String[]{
                SOURCE_FILE_PARAM,
                NO_OF_THREADS_PARAM
        };

        final String[] optional = new String[]{
        };

        parametersValidator.setRequiredKeys(required);
        parametersValidator.setOptionalKeys(optional);

        return parametersValidator;
    }

    @Bean
    @Qualifier("splitFilesStep")
    public Step splitFilesStep(@Qualifier("splitTasklet") Tasklet splitTasklet) {
        log.info("creating splitFilesStep step");
        return stepBuilderFactory.get("splitFilesStep")
                .tasklet(splitTasklet)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("splitTasklet")
    public Tasklet splitTasklet(@Value("#{jobParameters[" + SOURCE_FILE_PARAM + "]}") String filename,
                                @Value("#{jobParameters[" + NO_OF_THREADS_PARAM + "]}") String noOfThreads) {
        log.info("creating splitTasklet tasklet");
        return new FileSplitterTaskLet(filename, noOfThreads);
    }

    @Bean
    @Qualifier("encryptLines")
    public Step encryptLines(TaskExecutor taskExecutor,
                             MultiFileResourcePartioner partioner,
                             @Qualifier("encryptLineStep") Step encryptLineStep) throws UnExpectedBatchError {
        log.info("creating encryptLines step");
        return stepBuilderFactory.get("encryptLines")
                .partitioner("encryptLineStep", partioner)
                .step(encryptLineStep)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    @Qualifier("encryptLineStep")
    public Step encryptLineStep(EncryptionProcessor encryptProcessor) throws UnExpectedBatchError {
        log.info("creating encryptLine step");
        return stepBuilderFactory.get("encryptLineStep")
                .<String, String>chunk(1)
                .reader(itemReader(null))
                .processor(encryptProcessor)
                .writer(itemWriter( null))
                .build();
    }

    @Bean
    @StepScope
    public MultiFileResourcePartioner partioner(@Value("#{jobParameters[" + SOURCE_FILE_PARAM + "]}") String filename) {
        log.info("creating partioner step");
        return new MultiFileResourcePartioner(filename, resourcePatternResolver);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<String> itemReader(@Value("#{stepExecutionContext[fileName]}") String filename) throws UnExpectedBatchError {
        log.info("creating itemReader parallel step for file :" + filename);
        try {
            FlatFileItemReader<String> reader = new FlatFileItemReader<>();
            reader.setResource(new FileUrlResource(filename));
            reader.setLineMapper(new PassThroughLineMapper());
            return reader;
        } catch (MalformedURLException e) {
            throw new UnExpectedBatchError("Error occured creatign item reader", e);
        }
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<String> itemWriter(@Value("#{stepExecutionContext[opFileName]}") String filename) throws UnExpectedBatchError {
        log.info("creating itemWriter parallel step for file :" + filename);
        try {
            FlatFileItemWriter<String> writer = new FlatFileItemWriter<>();
            writer.setResource(new FileUrlResource(filename));
            writer.setLineAggregator(new PassThroughLineAggregator<>());
            return writer;
        } catch (MalformedURLException e) {
            throw new UnExpectedBatchError("Error occurred creating item writer", e);
        }
    }

    @Bean
    @Qualifier("mergeFiles")
    public Step mergeFiles(@Qualifier("joinTasklet") Tasklet joinTasklet) {
        log.info("creating mergeFiles step");
        return stepBuilderFactory.get("mergeFiles")
                .tasklet(joinTasklet)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("joinTasklet")
    public Tasklet joinTasklet(@Value("#{jobParameters[" + SOURCE_FILE_PARAM + "]}") String filename,
                               @Value("#{jobParameters[" + NO_OF_THREADS_PARAM + "]}") String noOfThreads,
                               @Value("${encrypt.output.filename}") String outputFileName) {
        log.info("creating joinTasklet Tasklet");
        return new FileJoinerTaskLet(filename, outputFileName, noOfThreads);
    }


    @Bean
    @StepScope
    public TaskExecutor taskExecutor(@Value("#{jobParameters[" + NO_OF_THREADS_PARAM + "]}") String noOfThreads)  {
        int threadsSize = Utils.convertToInt(noOfThreads, 5);
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threadsSize);
        taskExecutor.setCorePoolSize(threadsSize);
        taskExecutor.setQueueCapacity(threadsSize);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }
}