package com.wipro.batch.job;

import com.wipro.batch.exception.UnExpectedBatchError;
import com.wipro.batch.util.Utils;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiFileResourcePartioner implements Partitioner {

    private static final String DEFAULT_KEY_NAME = "fileName";

    private static final String PARTITION_KEY = "partition";

    private String keyName = DEFAULT_KEY_NAME;

    private ResourcePatternResolver resoursePatternResolver;

    private String fileName;

    public MultiFileResourcePartioner(String fileName, ResourcePatternResolver resoursePatternResolver) {
        this.fileName =
                fileName;
        this.resoursePatternResolver = resoursePatternResolver;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        try {
            Map<String, ExecutionContext> map = new HashMap<>(gridSize);
            Resource[] resources;
            try {
                resources = resoursePatternResolver.getResources("file:" + fileName + "_*");
            } catch (IOException e) {
                throw new UnExpectedBatchError("I/O problems when resolving the input file pattern.", e);
            }

            int i = 0;
            for (Resource resource : resources) {
                ExecutionContext context = new ExecutionContext();
                Assert.state(resource.exists(), "Resource does not exist: " + resource);
                context.putString(keyName, resource.getURI().getPath());
                context.putString("opFileName", Utils.createTmpOutputFilePath(resource.getFile()) + "_" + i);

                map.put(PARTITION_KEY + i, context);
                i++;
            }
            return map;
        } catch (Exception e) {
            throw new UnExpectedBatchError("Error occurred getting partitioned resources", e);
        }
    }
}
