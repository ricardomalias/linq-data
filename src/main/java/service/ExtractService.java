package service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.In;
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

            JavaPairRDD<String, Integer> filter = rowJavaRDD.mapToPair(item -> new Tuple2<>(item.getString(1), item))
                    .groupByKey()
                    .map(item -> {
                        ArrayList<Double> gb = new ArrayList<>();

                        Iterable<Row> rows = item._2();
                        rows.forEach(tr -> {
                            gb.add(tr.getDouble(4));
                        });

                        return new Tuple2<>(item._1(), gb);
                    })
                    .mapToPair(group -> {

                        int counter = 0;
                        for (int idx = 0; idx < group._2().size(); idx++) {
                            Double aDouble = group._2().get(idx);

                            if (idx < 2) {
                                continue;
                            }

                            if (aDouble < 10 && group._2().get(idx - 1) < 10 && group._2().get(idx - 2) > 10) {
                                counter++;
                            }
                        }

                        return new Tuple2<>(group._1(), counter);
                    })
                    .filter(group -> group._2() > 0);

            saveOutputFile(filter);

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
                .csv("/Users/ricardomalias/code/linq-data/src/main/resources/raw_data__acc_wir_chrg_dt_sum_usg__wireless_number.csv");
    }

    private void saveOutputFile(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws IOException {

        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD.collect();

        Path path = Paths.get("src", "main", "resources");
        File absoluteFile = path.toFile().getAbsoluteFile();

        if (appProps.get("save.output").equals("1")) {
            FileWriter writer = new FileWriter(absoluteFile + "/output.csv");
            writer.write("line, count_underneath" + System.lineSeparator());
            for(Tuple2<String, Integer> str: collect) {
                writer.write(str._1() + ", " + str._2() + System.lineSeparator());
            }
            writer.close();
        }
    }
}
