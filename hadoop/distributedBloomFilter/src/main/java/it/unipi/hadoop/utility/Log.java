package it.unipi.hadoop.utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;

public class Log {
    public static void writeLog(String file, String msg) {

        try {
            FileSystem hdfs = FileSystem.get(new Configuration());
            FSDataOutputStream dos;
            try {
                dos = hdfs.append(new Path("hdfs://hadoop-namenode:9820/user/hadoop/log/"+file));
            } catch (Exception e) {
                dos = hdfs.create(new Path("hdfs://hadoop-namenode:9820/user/hadoop/log/"+file), true);
            }
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));
            String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());

            br.write(timeStamp + "\t" + msg);
            br.newLine();

            br.close();
            hdfs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
