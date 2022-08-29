package it.unipi.hadoop.utility;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.io.Serializable;

@XStreamAlias("xmlConfig")
public class XMLconfig implements Serializable {
    private double falsePositiveRate;
    private String input;
    private Output output;
    private String root;

    public XMLconfig() {
    }

    public double getFalsePositiveRate() {
        return falsePositiveRate;
    }

    public void setFalsePositiveRate(double falsePositiveRate) {
        this.falsePositiveRate = falsePositiveRate;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    @Override
    public String toString() {
        return "XMLconfig{" +
                "falsePositiveRate=" + falsePositiveRate +
                ", input='" + input + '\'' +
                ", output=" + output +
                ", root='" + root + '\'' +
                '}';
    }
}

class Output implements Serializable {
    private String stage1;
    private String stage2;
    private String stage3;

    public Output() {
    }

    public String getStage1() {
        return stage1;
    }

    public void setStage1(String stage1) {
        this.stage1 = stage1;
    }

    public String getStage2() {
        return stage2;
    }

    public void setStage2(String stage2) {
        this.stage2 = stage2;
    }

    public String getStage3() {
        return stage3;
    }

    public void setStage3(String stage3) {
        this.stage3 = stage3;
    }

    @Override
    public String toString() {
        return "Output{" +
                "stage1='" + stage1 + '\'' +
                ", stage2='" + stage2 + '\'' +
                ", stage3='" + stage3 + '\'' +
                '}';
    }
}
