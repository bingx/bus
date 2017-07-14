package com.sibat.traffic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by User on 2017/7/7.
 */
public class WangguangCar {
    public static void main(String[] args) {
        DealDataBolt dealBolt = new DealDataBolt();
        String fileName = args[0].toString();
        File file = new File(fileName);
        if(!file.exists()){
            System.out.println("file is not exits");
            return ;
        }

        FileReader fileReader = null;
        try {
            fileReader = new FileReader(fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String str;
        // Open the reader
        BufferedReader reader = new BufferedReader(fileReader);

    }
}
