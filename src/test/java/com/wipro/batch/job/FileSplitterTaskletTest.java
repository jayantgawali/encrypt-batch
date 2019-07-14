package com.wipro.batch.job;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class FileSplitterTaskletTest {

    @Test
    public void splitFiles() throws Exception {
        File file = createInputData();
        FileSplitterTaskLet taskLet = new FileSplitterTaskLet(file.getPath(), "3");
        taskLet.execute(null, null);
        for(int i =0 ;i < 3 ; i++) {
            List<String> lines = Files.readAllLines(new File(file.getAbsolutePath() + "_" + i).toPath());
            Assert.assertEquals(lines.size(), 1);
            Assert.assertEquals(lines.get(0) , "Line" + (i+1));
        }
        cleanUpFiles(file);
    }

    private void cleanUpFiles(File file) {
        for(int i =0 ;i < 3 ; i++) {
            new File(file.getAbsolutePath() + "_" + i).delete();
        }
        file.delete();
    }

    private File createInputData() throws IOException {
        File file = new File("temp-input.txt");
        String input = "Line1\nLine2\nLine3";
        Files.write(file.toPath(), input.getBytes());
        return file;
    }

}
