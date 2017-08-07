package com.iss.demo;

import org.apache.avro.generic.GenericData;
import org.dmg.pmml.SeasonalTrendDecomposition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by hadoop on 2017/8/4 0004.
 */
public class Demo1 {
    public static void main(String [] args){
        readFileByLines("data/kmeans");
    }

    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static void readFileByLines(String fileName) {
        File file = new File(fileName);
        Map<String, List<String>> maps = new HashMap<String, List<String>>() ;
        BufferedReader reader = null;
        StringBuffer sb = new StringBuffer();
        try {

            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            System.out.println("以行为单位读取文件内容，一次读一整行f：");
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                String key = tempString.substring(tempString.length()-2);
                List<String> list = maps.get(key);
                if(list == null){
                    list = new ArrayList<String>();
                    maps.put(key, list);
                }
                list.add(tempString);

                //System.out.println("line " + line + ": " + tempString);
//                System.out.println("line " + line + ": " + tempString);
//                sb.append(",").append(tempString.trim().toUpperCase());
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }


        Set keyset = maps.keySet();
        Iterator iter = keyset.iterator();
        while(iter.hasNext()){
            String key = (String)iter.next();
            System.out.println(key);
            List list = maps.get(key);
            for (int i =0 ;i<list.size();i++){
                System.out.println(list.get(i));
            }
        }
    }
}
