package it.unipi.hadoop.utility;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.AnyTypePermission;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigManager {
    public static XMLconfig config;

    public ConfigManager() {
    }

    private static boolean XMLValidation(String xml, String xsd) {
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Document d = db.parse(new File(xml));
            Schema s = sf.newSchema(new StreamSource(xsd));
            s.newValidator().validate(new DOMSource(d));

        } catch(Exception e) {
            if(e instanceof SAXException)
                System.err.println("XML validation error: " + e.getMessage());
            else
                System.err.println(e.getMessage());

            return false;
        }

        return true;
    }

    public static boolean importConfig(String xmlPath, String xsdPath) {
        if (!XMLValidation(xmlPath, xsdPath)) {
            System.err.println("XML validation failed");
            return false;
        }

        XStream xs = new XStream();
        xs.processAnnotations(XMLconfig.class);
        xs.addPermission(AnyTypePermission.ANY);
        String importedXML;

        try {
            importedXML = new String(Files.readAllBytes(Paths.get(xmlPath)));
        } catch(Exception e) {
            System.err.println("XML loading failed");
            System.err.println(e.getMessage());
            return false;
        }
        XMLconfig newConfig = (XMLconfig)xs.fromXML(importedXML);
        System.out.println("Configuration loaded");
        config = newConfig;

        return true;
    }

    public static void printConfig() {
        System.out.println(config.toString());
    }

    public static double getFalsePositiveRate() {
        return config.getFalsePositiveRate();
    }

    public static String getInput() {
        return config.getInput();
    }

    public static String getOutputStage1() {
        return config.getOutput().getStage1();
    }

    public static String getOutputStage2() {
        return config.getOutput().getStage2();
    }

    public static String getOutputStage3() {
        return config.getOutput().getStage3();
    }

    public static String getRoot() {
        return config.getRoot();
    }
}
