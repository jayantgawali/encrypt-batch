package com.wipro.batch.job;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class FileJoinerTaskletTest {

    @Test
    public void testJoin() throws Exception{
        String pathname = createInputData();
        for(int i=0;i<3;i++) {
            File file = new File("temp/output_" + i);
            Files.write(file.toPath(), ("Line" + i).getBytes());
        }
        String path = new File(pathname).getPath();
        String outputFile = "result.txt";
        FileJoinerTaskLet taskLet = new FileJoinerTaskLet(path, outputFile, "3");
        taskLet.execute(null, null);
        File file = new File("temp/temp-input.txt-" + outputFile);
        Assert.assertTrue(file.exists());
        List<String> lines = Files.readAllLines(file.toPath());
        Assert.assertEquals(lines.size(), 3);
        cleanup(pathname);
    }

    private void cleanup(String pathname) {
        File folder = new File("temp/");
        File[] files = folder.listFiles();
        for(File file : files) {
            file.delete();
        }
        folder.delete();
    }

    private String createInputData() throws IOException {
        String pathname = "temp/temp-input.txt";
        new File("temp").mkdir();
        new File(pathname).createNewFile();
        return pathname;
    }
}
