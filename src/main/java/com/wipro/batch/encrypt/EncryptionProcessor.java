package com.wipro.batch.encrypt;

import com.wipro.batch.util.Utils;
import org.springframework.batch.item.ItemProcessor;

public class EncryptionProcessor implements ItemProcessor<String, String> {

    private int noOfShitChars;

    public EncryptionProcessor(String noOfCharacters) {
        this.noOfShitChars = Utils.convertToInt(noOfCharacters, 5);
    }

    @Override
    public String process(String text) throws Exception {
        StringBuffer result= new StringBuffer();
        for(char ch: text.toCharArray()) {
            result.append((char) (ch + noOfShitChars));
        }
        return result.toString();
    }
}
