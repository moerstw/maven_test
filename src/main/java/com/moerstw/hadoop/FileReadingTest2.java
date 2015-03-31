package com.moerstw.hadoop;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class FileReadingTest2 {
    /**
     *@param no
     *@return no
     */

    public static void main(String args[]) throws IOException {
        InputStream inp = null;
        try {
            inp = Class.forName("com.moerstw.hadoop.FileReadingTest2").getClassLoader().getResourceAsStream("D10C10T8N26.data");
            int c;
            // byte -> short -> int -> long -> float -> double
            byte buffer[] = new byte[4];
            while ((c = inp.read(buffer))!=-1){
                Byte b = new Byte(buffer[0]);
                System.out.println(b.intValue());

            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            if(inp != null) {
                inp.close();
            }
        }       
    }
 
}
