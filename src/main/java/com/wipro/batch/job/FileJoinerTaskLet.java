package com.wipro.batch.job;

import com.wipro.batch.exception.UnExpectedBatchError;
import com.wipro.batch.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FileJoinerTaskLet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(FileJoinerTaskLet.class);

    private String fileName;
    private int noOfFiles;
    private String outputFile;

    public FileJoinerTaskLet(String fileName, String outputFile, String noOfThreads) {
        this.fileName = fileName;
        this.noOfFiles = Utils.convertToInt(noOfThreads, 5);
        this.outputFile = outputFile;
    }


    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        log.info("Started joining the files");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName + "-" + outputFile))) {

            List<BufferedReader> readers = createReaders(fileName, noOfFiles);
            if (readers.size() != noOfFiles) {
                throw new UnExpectedBatchError("Error occurred joining files");
            }
            for(BufferedReader br: readers) {
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        bw.write(line);
                        bw.write(System.getProperty("line.separator"));
                    }
                } finally {
                    br.close();
                }
            }

        } catch (IOException e) {
            log.error("Error occurred handling files", e);
            throw e;
        }

        return RepeatStatus.FINISHED;
    }

    private List<BufferedReader> createReaders(String fileName, int noOfFiles) {
        File file = new File(fileName);
        return IntStream.range(0, noOfFiles).mapToObj(i -> {
            try {
                return new BufferedReader(new FileReader(Utils.createTmpOutputFilePath(file) + "_" + i));
            } catch (IOException e) {
                log.error("Error occurred creating readers", e);
            }
            return null;
        }).filter(b -> b!=null).collect(Collectors.toList());
    }
}
