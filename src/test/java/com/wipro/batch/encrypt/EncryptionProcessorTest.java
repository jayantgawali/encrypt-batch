package com.wipro.batch.encrypt;

import org.junit.Assert;
import org.junit.Test;

public class EncryptionProcessorTest {

    @Test
    public void testEncrypt() throws Exception{
        EncryptionProcessor encryptionProcessor = new EncryptionProcessor("5");
        String encryptedText = encryptionProcessor.process("Sample Text");
        Assert.assertEquals(encryptedText, "Xfruqj%Yj}y");
    }
}
