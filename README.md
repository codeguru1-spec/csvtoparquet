# csvtoparquet
package org.example;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import java.io.File;

public class CsvToParquetConverter {

    public static void convertCsvToParquet(String csvFilePath, String parquetFilePath) throws IOException {
        // Read CSV file
        FileReader fileReader = new FileReader(csvFilePath);

        // loads CSV file from the resource folder.
        //URL resource = CsvParserSimple.class.getClassLoader().getResource("csv/tcs-jul-23.csv");

        CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader());

        List<String> headers = csvParser.getHeaderNames();
        System.out.println("Headers: " + headers);

        // Dynamically generate Avro schema based on CSV headers
        Schema schema = generateAvroSchema(headers);

        // Create Avro Parquet writer
        Path path = new Path(parquetFilePath);
        Configuration conf = new Configuration();

        File myObj = new File(parquetFilePath);
        if (myObj.delete()) {
            System.out.println("Deleted the file: " + myObj.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }

        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(conf)
                .build();

        // Write CSV data to Parquet
        for (CSVRecord record : csvParser) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            for (String header : headers) {
                avroRecord.put(header, record.get(header));
            }
            writer.write(avroRecord);
        }

        writer.close();
        csvParser.close();
        fileReader.close();

        System.out.println("CSV file successfully converted to Parquet.");
    }

    private static Schema generateAvroSchema(List<String> headers) {
        // Use SchemaBuilder to build the schema
        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("CsvRecord").fields();
        for (String header : headers) {
            builder = builder.optionalString(header);
        }
        return builder.endRecord();
    }

    public static void main(String[] args) throws IOException {
        //if (args.length < 2) {
        //    System.err.println("Usage: CsvToParquetConverter <csv-file-path> <parquet-file-path>");
         //   System.exit(1);
       // }
        //String csvFilePath = args[0];
        //String parquetFilePath = args[1];
        // Set HADOOP_HOME and hadoop.home.dir programmatically

        System.setProperty("hadoop.home.dir", "C:\\hadoop");


        String csvFilePath = "C:\\Users\\sumit\\IdeaProjects\\CsvToParquetConverterJava\\src\\main\\resources\\positions.csv";
        String parquetFilePath = "C:\\Users\\sumit\\IdeaProjects\\CsvToParquetConverterJava\\src\\main\\resources\\output.parquet";
        convertCsvToParquet(csvFilePath, parquetFilePath);
    }
}


<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>csv-to-parquet-converter</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!-- Parquet Hadoop -->
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-hadoop</artifactId>
            <version>1.12.3</version>
        </dependency>

        <!-- Parquet Avro -->
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-avro</artifactId>
            <version>1.12.3</version>
        </dependency>

        <!-- Apache Commons CSV for CSV parsing -->
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-csv</artifactId>
            <version>1.10.0</version>
        </dependency>

        <!-- Hadoop common library -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>3.3.4</version>
        </dependency>

        <!-- Hadoop client library -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.3.4</version>
        </dependency>

        <!-- Logging dependencies -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.36</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.36</version>
        </dependency>

        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
        </dependency>

        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <version>1.11.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-common</artifactId>
            <version>1.12.0</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>

