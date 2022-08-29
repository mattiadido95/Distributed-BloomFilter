package it.unipi.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterBoolean implements Writable, Comparable<BloomFilterBoolean>{

    private int m;
    private int k;
    private boolean[] arrayBF;

    public BloomFilterBoolean(){}

    public BloomFilterBoolean(int m, int k) {
        this.m = m;
        this.k = k;
        this.arrayBF = new boolean[(int) m];
    }

    public BloomFilterBoolean(int m, int k, List<BloomFilterBoolean> arrayBFs) {
        this.m = m;
        this.k = k;
        this.arrayBF = new boolean[(int) m];
        for (BloomFilterBoolean bf : arrayBFs) {
            for (int i = 0; i < this.m; i++)
                this.arrayBF[i] |= bf.getArrayBF()[i];
        }
    }

    public void add(String title){
        int index;
        for(int i=0; i<k; i++){
            index = Math.abs(Hash.getInstance(Hash.MURMUR_HASH).hash(title.getBytes(StandardCharsets.UTF_8), i)) % m;
            arrayBF[index] = true;
        }
    }

    public boolean find(String title){
        int index;
        for(int i=0; i<k; i++) {
            index = Math.abs(Hash.getInstance(Hash.MURMUR_HASH).hash(title.getBytes(StandardCharsets.UTF_8), i)) % m;
            if (!arrayBF[index])
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        String result = "m : " + this.m + " k : " + this.k + " BloomFilter : \n";
        StringBuilder s = new StringBuilder();
        for (boolean b : arrayBF) {
            s.append(b ? 1 : 0);
        }
        return result + s;
    }

    public int compareTo(BloomFilterBoolean o) {
        if(this == o || (this.m == o.m && this.k == o.k && Arrays.equals(this.arrayBF, o.arrayBF)))
            return 0;
        return 1;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.m);
        out.writeInt(this.k);

        for (int i = 0; i < this.m; i++) {
            out.writeByte(this.arrayBF[i] ? 1 : 0);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.m = in.readInt();
        this.k = in.readInt();
        this.arrayBF = new boolean[this.m];

        for (int i = 0; i < this.m; i++) {
            arrayBF[i] = in.readByte() == 1;
        }
    }

    public int getM() {
        return m;
    }

    public void setM(int m) {
        this.m = m;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public boolean[] getArrayBF() {
        return arrayBF;
    }

    public void setArrayBF(boolean[] arrayBF) {
        this.arrayBF = arrayBF;
    }
}