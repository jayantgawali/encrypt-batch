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

public class FileSplitterTaskLet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(FileSplitterTaskLet.class);

    private String fileName;
    private int noOfFiles;

    public FileSplitterTaskLet(String fileName, String noOfThreads) {
        this.fileName = fileName;
        this.noOfFiles = Utils.convertToInt(noOfThreads, 5);
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        log.info("Started splitting the files");
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            List<BufferedWriter> writers = createWriters(fileName, noOfFiles);
            if(writers.size() != noOfFiles) {
                throw new UnExpectedBatchError("Error occurred splitting files");
            }
            try {
                String line;
                int index = 0;
                while ((line = br.readLine()) != null) {
                    if(index >= noOfFiles) {
                        index = 0;
                    }
                    writers.get(index).write(line);
                    writers.get(index).write(System.getProperty("line.separator"));
                    index++;
                }
            } finally {
                writers.forEach(w -> {
                    try {
                        w.close();
                    } catch (IOException e) {
                        log.error("Error occurred closing file", e);
                    }
                });
            }
        } catch (IOException e) {
            log.error("Error occurred handling files", e);
            throw e;
        }


        return RepeatStatus.FINISHED;
    }

    private List<BufferedWriter> createWriters(String fileName, int noOfFiles) {
        return IntStream.range(0, noOfFiles).mapToObj(i -> {
            try {
                return new BufferedWriter(new FileWriter(fileName + "_" + i));
            } catch (IOException e) {
                log.error("Error occurred creating writes", e);
            }
            return null;
        }).filter(b -> b!=null).collect(Collectors.toList());
    }
}
