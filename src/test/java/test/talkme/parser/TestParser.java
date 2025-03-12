package test.talkme.parser;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.*;

import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
//import org.apache.parquet.schema.Types;
import talkme.parser.ParquetParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Collections;
import java.util.List;




class TestParser {
    private static ParquetParser parser;

    @BeforeAll
    static void setup() throws IOException {

        String testFile = "data/yellow_tripdata_2009-01.parquet";
        parser= new ParquetParser(testFile,5,0,10);
    }

    @Test
    void testNames(){
        List<String> listTest= new ArrayList<>(List.of("vendor_name", "Trip_Pickup_DateTime", "Trip_Dropoff_DateTime", "Passenger_Count", "Trip_Distance", "Start_Lon", "Start_Lat", "Rate_Code", "store_and_forward", "End_Lon", "End_Lat", "Payment_Type", "Fare_Amt", "surcharge", "mta_tax", "Tip_Amt", "Tolls_Amt", "Total_Amt"));
        assertIterableEquals(listTest, parser.getColumnNames());
    }

    @Test
    void testTypes() {
        // Create a list of expected Parquet types with column names
        List<Type> listTest = new ArrayList<>();
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "vendor_name (STRING)"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "Trip_Pickup_DateTime (STRING)"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "Trip_Dropoff_DateTime (STRING)"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "Passenger_Count"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Trip_Distance"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Start_Lon"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Start_Lat"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Rate_Code"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "store_and_forward"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "End_Lon"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "End_Lat"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "Payment_Type (STRING)"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Fare_Amt"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "surcharge"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "mta_tax"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Tip_Amt"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Tolls_Amt"));
        listTest.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "Total_Amt"));

        List<String> expectedTypes = listTest.stream().map(Type::toString).toList();
        List<String> actualTypes = parser.getColumnTypes().stream().map(Type::toString).toList();
        assertIterableEquals(expectedTypes, actualTypes);
    }

    @Test
    void testBatches() throws IOException {
        Object[][] testBatch1 = {
                {"VTS", "2009-01-04 02:52:00", "2009-01-04 03:02:00", "1", "2.63", "-73.991957", "40.721567", null, null, "-73.993803", "40.695922", "CASH", "8.9", "0.5", null, "0.0", "0.0", "9.4"},
                {"VTS", "2009-01-04 03:31:00", "2009-01-04 03:38:00", "3", "4.55", "-73.982102", "40.73629", null, null, "-73.95585", "40.76803", "Credit", "12.1", "0.5", null, "2.0", "0.0", "14.6"},
                {"VTS", "2009-01-03 15:43:00", "2009-01-03 15:57:00", "5", "10.35", "-74.002587", "40.739748", null, null, "-73.869983", "40.770225", "Credit", "23.7", "0.0", null, "4.74", "0.0", "28.44"},
                {"DDS", "2009-01-01 20:52:58", "2009-01-01 21:14:00", "1", "5.0", "-73.974267", "40.790955", null, null, "-73.99655799999998", "40.731849", "CREDIT", "14.9", "0.5", null, "3.05", "0.0", "18.45"},
                {"DDS", "2009-01-24 16:18:23", "2009-01-24 16:24:56", "1", "0.4", "-74.00158", "40.719382", null, null, "-74.00837799999998", "40.72035", "CASH", "3.7", "0.0", null, "0.0", "0.0", "3.7"}
        };


        Object[][] testBatch2 = {
                {"DDS", "2009-01-16 22:35:59", "2009-01-16 22:43:35", "2", "1.2", "-73.989806", "40.735006", null, null, "-73.985021", "40.724494", "CASH", "6.1", "0.5", null, "0.0", "0.0", "6.6"},
                {"DDS", "2009-01-21 08:55:57", "2009-01-21 09:05:42", "1", "0.4", "-73.98404999999998", "40.743544", null, null, "-73.98026", "40.748926", "CREDIT", "5.7", "0.0", null, "1.0", "0.0", "6.7"},
                {"VTS", "2009-01-04 04:31:00", "2009-01-04 04:36:00", "1", "1.72", "-73.992635", "40.748362", null, null, "-73.995585", "40.728307", "CASH", "6.1", "0.5", null, "0.0", "0.0", "6.6"},
                {"CMT", "2009-01-05 16:29:02", "2009-01-05 16:40:21", "1", "1.6", "-73.96969", "40.749244", null, null, "-73.990413", "40.751082", "Credit", "8.699999999999998", "0.0", null, "1.3", "0.0", "10.0"},
                {"CMT", "2009-01-05 18:53:13", "2009-01-05 18:57:45", "1", "0.6999999999999998", "-73.955173", "40.783044", null, null, "-73.95859799999998", "40.774822", "Cash", "5.9", "0.0", null, "0.0", "0.0", "5.9"}
        };


        List<List<Object>> listTest1 = new ArrayList<>();
        for (Object[] row : testBatch1) {
            listTest1.add(new ArrayList<>(Arrays.asList(row)));
        }
        List<List<Object>> listTest2 = new ArrayList<>();
        for (Object[] row : testBatch2) {
            listTest2.add(new ArrayList<>(Arrays.asList(row)));
        }


        assertIterableEquals(listTest1, parser.getNextBatch());
        assertIterableEquals(listTest2, parser.getNextBatch());
        assertIterableEquals(new ArrayList<>(), parser.getNextBatch());
    }


    @AfterAll
    static void done() throws IOException {
        parser.close();
    }
}
