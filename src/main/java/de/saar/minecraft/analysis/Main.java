package de.saar.minecraft.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        AnalysisConfiguration config;
        try {
            config = AnalysisConfiguration.loadYaml(new FileReader("config.yml"));
        } catch (FileNotFoundException e) {
            logger.error("Configuration file not found. {}", e.getMessage());
            return;
        }
        var x = new ExperimentAnalysis(config);
        x.makeAnalysis();
    }

}
