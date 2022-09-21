package it.unipi.hadoop.utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.SimpleDateFormat;

public class Log {
    public static void writeLog(String file, String msg) {
        try {
            FileSystem hdfs = FileSystem.get(new Configuration());
            FSDataOutputStream dos;
            try {
                dos = hdfs.append(new Path("hdfs://hadoop-namenode:9820/user/hadoop/log/" + file));
            } catch (FileNotFoundException e) {
                dos = hdfs.create(new Path("hdfs://hadoop-namenode:9820/user/hadoop/log/" + file), true);
            }
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));
            String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());

            br.write(timeStamp + "\t" + msg);
            br.newLine();

            br.close();
            // hdfs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeLocal(String file, String msg) throws IOException {
        BufferedWriter out = null;
        try {
            FileWriter fstream = new FileWriter(file, true); //true tells to append data.
            out = new BufferedWriter(fstream);
            out.write(msg + "\n");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
