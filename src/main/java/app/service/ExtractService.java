package app.service;

import app.model.ExtractionLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ExtractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractService.class);
    private final String filePath;
    private final Properties appProps;
    private final SparkConf sparkConf;
    private SparkSession sparkSession;

    public ExtractService(String filePath, Properties appProps, SparkConf sparkConf) {
        this.filePath = filePath;
        this.appProps = appProps;
        this.sparkConf = sparkConf;
    }

    private void createSession() {
        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    public void extractPhrase() {
        createSession();

        if (filePath.isEmpty()) {
            LOGGER.error("Error on load file, please provide file absolute path as application argument");
            System.exit(1);
        }

        try {
            SQLContext sqlContext = sparkSession.sqlContext();
            Dataset<Row> datasetWirelessNumberAttribute = getDatasetWirelessNumberAttribute();

            datasetWirelessNumberAttribute.createOrReplaceTempView("wireless_number_attribute");

            Dataset<Row> sql = sqlContext.sql("SELECT " +
                    " COUNT(*) AS total, " +
                    " CAST(rd_wireless_number AS STRING) AS line, " +
                    " UPPER(SUBSTRING(rd_bill_cycle_date,1,3)) AS month, " +
                    " FLOOR(INSTR('JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC', UPPER(SUBSTRING(rd_bill_cycle_date,1,3)))/4+1) AS month_index, " +
                    " SUM(rd_used) AS gigabytes " +
                    "FROM " +
                    " wireless_number_attribute " +
                    "WHERE " +
                    " rd_item_description = 'Monthly Data Plan' " +
                    " GROUP BY rd_wireless_number, rd_bill_cycle_date " +
                    " ORDER BY month_index ASC "
            );

            JavaRDD<Row> rowJavaRDD = sql.toJavaRDD();

            List<ExtractionLine> collect1 = rowJavaRDD.mapToPair(item -> new Tuple2<>(item.getString(1), item))
                    .groupByKey()
                    .map(item -> {
                        ArrayList<Double> gb = new ArrayList<>();

                        Iterable<Row> rows = item._2();
                        rows.forEach(tr -> {
                            gb.add(tr.getDouble(4));
                        });

                        return new Tuple2<>(item._1(), gb);
                    })
                    .filter(group -> {
                        List<Double> collect = group._2().stream().filter(item -> item > 10).collect(Collectors.toList());
                        return collect.size() > 1;
                    })
                    .collect()
                    .stream()
                    .map(group -> {
                        int dropped = 0;
                        int underneath = 0;
                        int percentage = 0;
                        for (int idx = 0; idx < group._2().size(); idx++) {
                            Double aDouble = group._2().get(idx);

                            if (idx < 1) {
                                continue;
                            }

                            if (aDouble < 10 && group._2().get(idx - 1) > 10 && idx < group._2().size() - 1) {
                                dropped++;
                            }

                            if (idx < 2) {
                                continue;
                            }

                            if (aDouble < 10 && group._2().get(idx - 1) < 10 && group._2().get(idx - 2) > 10) {
                                underneath++;
                            }
                        }

                        if (dropped > 0) {
                            percentage = (underneath * 100) / dropped;
                        }

                        LOGGER.info("teste group: {} | dropped: {} | underneath: {}, percentage: {}", group, dropped, underneath, percentage);

                        return new ExtractionLine(group._1(), group._2(), dropped, underneath, percentage);
                    })
                    .collect(Collectors.toList());

            saveOutputFile(collect1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Dataset<Row> getDatasetGeneral() {
        return sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);
    }

    private Dataset<Row> getDatasetWirelessNumber() {
        return sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/ricardomalias/code/linq-data/src/main/resources/wireless_number__attribute.csv");
    }

    private Dataset<Row> getDatasetWirelessNumberAttribute() {
        return sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/ricardomalias/code/linq-data/src/main/resources/raw_data_impostor.csv");
    }

    private void saveOutputFile(List<ExtractionLine> collect) throws IOException {

//        List<ExtractionLine> collect = stringIntegerJavaPairRDD.collect();

        Path path = Paths.get("src", "main", "resources");
        File absoluteFile = path.toFile().getAbsoluteFile();

        if (appProps.get("save.output").equals("1")) {
            FileWriter writer = new FileWriter(absoluteFile + "/output2.csv");
            FileWriter writerDetail = new FileWriter(absoluteFile + "/output_detail2.csv");

            writer.write("line_number | percentage " + System.lineSeparator());
            writerDetail.write("line_number | gigabytes | dropped | kept_underneath | percentage " + System.lineSeparator());

            for(ExtractionLine str: collect) {
                writer.write(
                        str.getLineNumber() +
                        " | " +
                        str.getPercentage() +
                        "%" +
                        System.lineSeparator()
                );

                writerDetail.write(
                        str.getLineNumber() +
                        " | " +
                        str.getGigabytes() +
                        " | " +
                        str.getDropped() +
                        " | " +
                        str.getUnderneath() +
                        " | " +
                        str.getPercentage() +
                        "%" +
                        System.lineSeparator()
                );
            }

            writer.close();
            writerDetail.close();
        }
    }
}
