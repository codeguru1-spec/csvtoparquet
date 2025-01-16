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
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
public class CsvToParquetConverter {

    public static void convertCsvToParquet(String csvFilePath, String parquetFilePath) throws IOException {
        try (FileReader fileReader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            // Get CSV headers
            List<String> headers = csvParser.getHeaderNames();
            System.out.println("Converting file: " + csvFilePath);
            System.out.println("Headers: " + headers);

            // Generate Avro schema from CSV headers and get header mapping
            Map<String, String> headerMap = new HashMap<>();
            Schema schema = generateAvroSchema(headers, headerMap);

            // Create Avro Parquet writer
            Path path = new Path(parquetFilePath);
            Configuration conf = new Configuration();

            // Delete the Parquet file if it already exists
            File outputFile = new File(parquetFilePath);
            if (outputFile.delete()) {
                System.out.println("Deleted existing Parquet file: " + outputFile.getName());
            }

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(conf)
                    .build()) {

                // Write CSV data to Parquet
                for (CSVRecord record : csvParser) {
                    GenericRecord avroRecord = new GenericData.Record(schema);
                    for (String header : headers) {
                        // Use the sanitized header to put data into the Avro record
                        String sanitizedHeader = headerMap.get(header);
                        avroRecord.put(sanitizedHeader, record.get(header));
                    }
                    writer.write(avroRecord);
                }
            }

            System.out.println("Converted CSV to Parquet: " + parquetFilePath);
        }
    }

    private static Schema generateAvroSchema(List<String> headers, Map<String, String> headerMap) {
        // Build Avro schema based on sanitized CSV headers
        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("CsvRecord").fields();
        for (String header : headers) {
            // Sanitize the header by replacing spaces with underscores
            String sanitizedHeader = header.replaceAll("\\s+", "_");
            headerMap.put(header, sanitizedHeader);
            builder = builder.optionalString(sanitizedHeader);
        }
        return builder.endRecord();
    }

    public static void convertAllCsvFilesInFolder(String inputFolderPath, String outputFolderPath) throws IOException {
        // Create output folder if it doesn't exist
        File outputFolder = new File(outputFolderPath);
        if (!outputFolder.exists()) {
            outputFolder.mkdir();
        }

        // List all CSV files in the input folder
        File inputFolder = new File(inputFolderPath);
        File[] csvFiles = inputFolder.listFiles((dir, name) -> name.endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            System.out.println("No CSV files found in the input folder.");
            return;
        }

        // Process each CSV file
        for (File csvFile : csvFiles) {
            String csvFilePath = csvFile.getAbsolutePath();
            String parquetFilePath = outputFolderPath + "/" + csvFile.getName().replace(".csv", ".parquet");

            try {
                // Convert each CSV file to Parquet
                convertCsvToParquet(csvFilePath, parquetFilePath);
            } catch (Exception e) {
                // Handle any exception that occurs while processing the current CSV file
                System.err.println("Error processing file: " + csvFilePath + ". Skipping to the next file.");
                e.printStackTrace();
            }
        }

        System.out.println("All valid CSV files have been processed.");
    }

    public static void main(String[] args) throws IOException {
        // Set input and output folder paths
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        String inputFolderPath = "C:\\Users\\sumit\\IdeaProjects\\CsvToParquetConverterJava\\src\\main\\resources\\input";
        String outputFolderPath = "C:\\Users\\sumit\\IdeaProjects\\CsvToParquetConverterJava\\src\\main\\resources\\output";

        // Convert all CSV files in the input folder
        convertAllCsvFilesInFolder(inputFolderPath, outputFolderPath);
    }
}
