package app;

import app.config.PropertyConfig;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import app.service.ExtractService;

import java.util.Properties;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {

        LOGGER.info("file loaded {}", args[0]);

        startExtraction(args[0]);
    }

    private static void startExtraction(String filePath) {
        LOGGER.info("----- START COUNTER -----");

        Properties appProps = PropertyConfig.getProperties();
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("DataExtraction");

        ExtractService phraseService = new ExtractService(filePath, appProps, sparkConf);
        phraseService.extractPhrase();
    }
}
