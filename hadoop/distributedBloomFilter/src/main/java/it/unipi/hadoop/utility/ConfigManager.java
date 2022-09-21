package it.unipi.hadoop.utility;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;

public class ConfigManager {
    private static JSONObject config;

    public ConfigManager() {
    }

    @SuppressWarnings("unchecked")
    public static boolean importConfig(String jsonPath) {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject;
        try {
            Object obj = parser.parse(new FileReader(jsonPath));
            config = (JSONObject) obj;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static void printConfig() {
        System.out.println(config.toString());
    }

    public static double getFalsePositiveRate() { return (double) config.get("falsePositiveRate"); }

    public static String getInput() {
        return (String) config.get("input");
    }

    public static String getOutputStage1() {
        JSONObject output = (JSONObject) config.get("output");
        return (String) output.get("stage1");
    }

    public static String getOutputStage2() {
        JSONObject output = (JSONObject) config.get("output");
        return (String) output.get("stage2");
    }

    public static String getOutputStage3() {
        JSONObject output = (JSONObject) config.get("output");
        return (String) output.get("stage3");
    }

    public static int getLinesPerMapStage1() {
        JSONObject output = (JSONObject) config.get("linesPerMap");
        return ((Long) output.get("stage1")).intValue();
    }

    public static int getLinesPerMapStage2() {
        JSONObject output = (JSONObject) config.get("linesPerMap");
        return ((Long) output.get("stage2")).intValue();
    }

    public static int getLinesPerMapStage3() {
        JSONObject output = (JSONObject) config.get("linesPerMap");
        return ((Long) output.get("stage3")).intValue();
    }

    public static int getNReducerStage1() {
        JSONObject output = (JSONObject) config.get("nReducer");
        return ((Long) output.get("stage1")).intValue();
    }

    public static int getNReducerStage2() {
        JSONObject output = (JSONObject) config.get("nReducer");
        return ((Long) output.get("stage2")).intValue();
    }

    public static int getNReducerStage3() {
        JSONObject output = (JSONObject) config.get("nReducer");
        return ((Long) output.get("stage3")).intValue();
    }

    public static String getRoot() {
        return (String) config.get("root");
    }

    public static String getStatsFile() {
        return (String) config.get("statsFile");
    }

    public static String getOutputFile() {
        return (String) config.get("outputFile");
    }
}
