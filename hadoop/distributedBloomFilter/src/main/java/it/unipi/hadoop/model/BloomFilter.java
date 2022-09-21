package it.unipi.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilter implements Writable, Comparable<BloomFilter> {

    /*
    m = number of bit of the bloom filter
    k = number of hash functions
    arrayBF = BitSet structure representing our bloom filter (store indexes of set positions)
     */
    private int m;
    private int k;
    private BitSet arrayBF;

    // used in deserialization phase
    public BloomFilter() {
    }

    public BloomFilter(int m, int k) { // standard constructor
        this.m = m;
        this.k = k;
        this.arrayBF = new BitSet(m);
    }

    // used to create a bloom filter from an iterator of bloom filters
    public BloomFilter(Iterable<BloomFilter> arrayBFs) { // constructor to create new bloom filter from merge of bloomfilter list
        BloomFilter first = arrayBFs.iterator().next();
        // take params from first bloom filter beacuse are the same for all bf of arrayBFs list
        this.m = first.m;
        this.k = first.k;
        this.arrayBF = (BitSet) first.arrayBF.clone(); // take first bf to inizialize final bf

        while (arrayBFs.iterator().hasNext()) {
            or(arrayBFs.iterator().next().getArrayBF()); // compute or operation
        }
    }

    // compute k MURMUR_HASH for a given title and set relative bits in bloom filter
    public void add(String title) {
        int index;
        for (int i = 0; i < k; i++) { // compute k hash function
            index = Math.abs(Hash.getInstance(Hash.MURMUR_HASH).hash(title.getBytes(StandardCharsets.UTF_8), i)) % m;
            arrayBF.set(index, true);
        }
    }

    // return true only if it finds all the k bits, associated to the title, set
    public boolean find(String title) {
        int index;
        for (int i = 0; i < k; i++) { // compute k hash function
            index = Math.abs(Hash.getInstance(Hash.MURMUR_HASH).hash(title.getBytes(StandardCharsets.UTF_8), i)) % m;
            if (arrayBF.get(index) == false)
                return false;
        }
        return true;
    }

    public void or(BitSet input) {
        this.arrayBF.or(input);
    }

    @Override
    public String toString() {
        String result = "m : " + this.m + " k : " + this.k + " BloomFilter : \n";
        StringBuilder s = new StringBuilder();
        // transform BitSet representation to array boolean representation
        for (int i = 0; i < arrayBF.length(); i++) {
            s.append(arrayBF.get(i) == true ? 1 : 0);
        }
        return result + s;
    }

    public int compareTo(BloomFilter o) {
        if (this == o || (this.m == o.m && this.k == o.k && this.arrayBF.equals(o.arrayBF)))
            return 0;
        return 1;
    }

    public void write(DataOutput out) throws IOException {
        // how serialize bf on file system
        out.writeInt(this.m); // as int
        out.writeInt(this.k); // as int
        byte[] bytes = arrayBF.toByteArray(); // bitset array to byte array

        out.writeInt(bytes.length); // len of bf in byte
        for (int i = 0; i < bytes.length; i++) {
            out.writeByte(bytes[i]);
        }
    }

    public void readFields(DataInput in) throws IOException {
        // read from file system
        this.m = in.readInt(); // read as int m
        this.k = in.readInt(); // read as int k
        int length = in.readInt(); // read len in byte of bf
        byte[] bytes = new byte[length]; // creation of byte array for bf

        for (int i = 0; i < length; i++) {
            bytes[i] = in.readByte();
        }
        this.arrayBF = BitSet.valueOf(bytes); // convert byte array into bitset array
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

    public BitSet getArrayBF() {
        return this.arrayBF;
    }

    public void setArrayBF(BitSet arrayBF) {
        this.arrayBF = arrayBF;
    }
}
