package test.talkme.parser;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.*;

import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
//import org.apache.parquet.schema.Types;
import talkme.parser.ParquetParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Collections;
import java.util.List;




import org.junit.jupiter.api.*;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class TestParser {
    private static ParquetParser parser;

    @BeforeAll
    static void setup() throws IOException {
        String testFile = "data/yellow_tripdata_2009-01.parquet";
        parser = new ParquetParser(new File(testFile), 5);
    }

    @Test
    void testNames() {
        List<String> listTest = new ArrayList<>(List.of(
                "vendor_name", "Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Passenger_Count",
                "Trip_Distance", "Start_Lon", "Start_Lat", "Rate_Code", "store_and_forward",
                "End_Lon", "End_Lat", "Payment_Type", "Fare_Amt", "surcharge", "mta_tax",
                "Tip_Amt", "Tolls_Amt", "Total_Amt"
        ));
        assertIterableEquals(listTest, parser.getColumnNames());
    }

    @Test
    void testTypes() {
        List<String> expectedTypes = List.of(
                "BINARY", "BINARY", "BINARY", "INT64", "DOUBLE",
                "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE",
                "DOUBLE", "BINARY", "DOUBLE", "DOUBLE", "DOUBLE",
                "DOUBLE", "DOUBLE", "DOUBLE"
        );

        List<String> actualTypes = parser.getColumnTypes().stream()
                .map(type -> ((PrimitiveType) type).getPrimitiveTypeName().name())
                .toList();
        assertIterableEquals(expectedTypes, actualTypes);
    }

    @Test
    void testBatches() throws IOException {
        // Expected column-major structure (each inner list is a column, not a row)
        List<List<Object>> expectedBatch1 = List.of(
                List.of("VTS", "VTS", "VTS", "DDS", "DDS"), // vendor_name
                List.of("2009-01-04 02:52:00", "2009-01-04 03:31:00", "2009-01-03 15:43:00", "2009-01-01 20:52:58", "2009-01-24 16:18:23"), // Pickup times
                List.of("2009-01-04 03:02:00", "2009-01-04 03:38:00", "2009-01-03 15:57:00", "2009-01-01 21:14:00", "2009-01-24 16:24:56"), // Dropoff times
                List.of("1", "3", "5", "1", "1"), // Passenger count
                List.of("2.63", "4.55", "10.35", "5.0", "0.4"), // Trip distance
                List.of("-73.991957", "-73.982102", "-74.002587", "-73.974267", "-74.00158"), // Start Lon
                List.of("40.721567", "40.73629", "40.739748", "40.790955", "40.719382"), // Start Lat
                new ArrayList<>(Arrays.asList(null, null, null, null, null)), // Rate Code
                new ArrayList<>(Arrays.asList(null, null, null, null, null)), // store_and_forward
                List.of("-73.993803", "-73.95585", "-73.869983", "-73.99655799999998", "-74.00837799999998"), // End Lon
                List.of("40.695922", "40.76803", "40.770225", "40.731849", "40.72035"), // End Lat
                List.of("CASH", "Credit", "Credit", "CREDIT", "CASH"), // Payment Type
                List.of("8.9", "12.1", "23.7", "14.9", "3.7"), // Fare Amt
                List.of("0.5", "0.5", "0.0", "0.5", "0.0"), // Surcharge
                new ArrayList<>(Arrays.asList(null, null, null, null, null)), // mta_tax
                List.of("0.0", "2.0", "4.74", "3.05", "0.0"), // Tip Amt
                List.of("0.0", "0.0", "0.0", "0.0", "0.0"), // Tolls Amt
                List.of("9.4", "14.6", "28.44", "18.45", "3.7") // Total Amt
        );

        List<List<Object>> expectedBatch2 = List.of(
                List.of("DDS", "DDS", "VTS", "CMT", "CMT"), // vendor_name
                List.of("2009-01-16 22:35:59", "2009-01-21 08:55:57", "2009-01-04 04:31:00", "2009-01-05 16:29:02", "2009-01-05 18:53:13"), // Pickup times
                List.of("2009-01-16 22:43:35", "2009-01-21 09:05:42", "2009-01-04 04:36:00", "2009-01-05 16:40:21", "2009-01-05 18:57:45"), // Dropoff times
                List.of("2", "1", "1", "1", "1"), // Passenger count
                List.of("1.2", "0.4", "1.72", "1.6", "0.6999999999999998"), // Trip distance
                List.of("-73.989806", "-73.98404999999998", "-73.992635", "-73.96969", "-73.955173"), // Start Lon
                List.of("40.735006", "40.743544", "40.748362", "40.749244", "40.783044"), // Start Lat
                new ArrayList<>(Arrays.asList(null, null, null, null, null)), // Rate Code
                new ArrayList<>(Arrays.asList(null, null, null, null, null)), // store_and_forward
                List.of("-73.985021", "-73.98026", "-73.995585", "-73.990413", "-73.95859799999998"), // End Lon
                List.of("40.724494", "40.748926", "40.728307", "40.751082", "40.774822"), // End Lat
                List.of("CASH", "CREDIT", "CASH", "Credit", "Cash"), // Payment Type
                List.of("6.1", "5.7", "6.1", "8.699999999999998", "5.9"), // Fare Amt
                List.of("0.5", "0.0", "0.5", "0.0", "0.0"), // Surcharge
                new ArrayList<>(Arrays.asList(null, null, null, null, null)), // mta_tax
                List.of("0.0", "1.0", "0.0", "1.3", "0.0"), // Tip Amt
                List.of("0.0", "0.0", "0.0", "0.0", "0.0"), // Tolls Amt
                List.of("6.6", "6.7", "6.6", "10.0", "5.9") // Total Amt
        );

        assertIterableEquals(expectedBatch1, parser.getNextBatch());
        assertIterableEquals(expectedBatch2, parser.getNextBatch());
        List<List<Object>> emptyList = new ArrayList<>();
        for (int i = 0; i < 18; i++) {
            emptyList.add(new ArrayList<>());
        }
        assertIterableEquals(emptyList, parser.getNextBatch()); // Ensure no more data
    }

    @AfterAll
    static void done() throws IOException {
        parser.close();
    }
}
