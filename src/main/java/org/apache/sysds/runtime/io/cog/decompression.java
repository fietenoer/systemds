package org.apache.sysds.runtime.io.cog;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;


//TODO: need help over here
public class decompression {
    private static final int CLEAR_CODE = 256;
    private static final int END_CODE = 257;
    private static final int INITIAL_DICT_SIZE = 258;
    byte[] decompressed;
    private static class BitRead {
        private final ByteArrayInputStream input;
        private int currentByte;
        private int bitPosition;

        public BitRead(ByteArrayInputStream input) {
            this.input = input;
            this.currentByte = 0;
            this.bitPosition = 8;
        }

        public int readBits(int numBits) throws IOException {
            int value = 0;
            for (int i = 0; i < numBits; i++) {
                if (bitPosition == 8) {
                    currentByte = input.read();
                    if (currentByte == -1) {
                        throw new IOException("Unexpected end of stream.");
                    }
                    bitPosition = 0;
                }
                value |= ((currentByte >> (7 - bitPosition)) & 1) << (numBits - 1 - i);
                bitPosition++;
            }
            return value;
        }
    }
    public void decompress(byte[] compressedData) throws IOException {
        ArrayList<byte[]> dictionary = new ArrayList<>();
        int dictSize = INITIAL_DICT_SIZE;

        // Initialize dictionary with single-byte entries
        for (int i = 0; i < 256; i++) {
            dictionary.add(new byte[]{(byte) i});
        }

        ByteArrayInputStream inputStream = new ByteArrayInputStream(compressedData);
        BitRead bitReader = new BitRead(inputStream);

        int codeLength = 9; // Start with 9-bit codes
        int prevCode = -1;

        ArrayList<Byte> output = new ArrayList<>();

        while (true) {
            int code = bitReader.readBits(codeLength);
            if (code == END_CODE) {
                break;
            } else if (code == CLEAR_CODE) {
                // Reset the dictionary
                dictionary = new ArrayList<>();
                for (int i = 0; i < 256; i++) {
                    dictionary.add(new byte[]{(byte) i});
                }
                dictSize = INITIAL_DICT_SIZE;
                codeLength = 9;
                prevCode = -1;
                continue;
            }

            byte[] entry;
            if (code < dictionary.size()) {
                entry = dictionary.get(code);
            } else if (code == dictSize && prevCode != -1) {
                byte[] prevEntry = dictionary.get(prevCode);
                entry = new byte[prevEntry.length + 1];
                System.arraycopy(prevEntry, 0, entry, 0, prevEntry.length);
                entry[prevEntry.length] = prevEntry[0];
            } else {
                throw new IOException("Invalid LZW code.");
            }

            // Add entry to output
            for (byte b : entry) {
                output.add(b);
            }

            // Add new entry to dictionary
            if (prevCode != -1 && dictSize < 4096) {
                byte[] prevEntry = dictionary.get(prevCode);
                byte[] newEntry = new byte[prevEntry.length + 1];
                System.arraycopy(prevEntry, 0, newEntry, 0, prevEntry.length);
                newEntry[prevEntry.length] = entry[0];
                dictionary.add(newEntry);
                dictSize++;

                // Increase code length if necessary
                if (dictSize == (1 << codeLength) && codeLength < 12) {
                    codeLength++;
                }
            }

            prevCode = code;
        }

        // Convert output to byte array
        byte[] decompressedData = new byte[output.size()];
        for (int i = 0; i < output.size(); i++) {
            decompressedData[i] = output.get(i);
        }

        this.decompressed =  decompressedData;
    }

    public void createJob(byte[] data){
        SparkConf conf = new SparkConf()
                .setAppName("TIFF parsing")
                .setMaster("local[*]") // Use local mode with all cores
                .set("spark.executor.memory", "4g") // Executor memory
                .set("spark.executor.cores", "2"); // Number of executor cores

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();


        SparkContext sc = new SparkContext(conf); // create a new spark context to read the binary file.

        // Example GeoTIFF byte data (replace with actual data)
        byte[] geoTIFFBytes = {/* your byte array data */};

        // Split byte array into chunks for parallel processing
        int chunkSize = 1024; // Adjust chunk size based on your processing needs
        int numChunks = (int) Math.ceil((double) geoTIFFBytes.length / chunkSize);
        // Load a simple text file and count the words
        JavaRDD<byte[]> byteRDD = sc.parallelize(
                java.util.stream.IntStream.range(0, numChunks)
                        .mapToObj(i -> {
                            int start = i * chunkSize;
                            int end = Math.min(start + chunkSize, geoTIFFBytes.length);
                            byte[] chunk = new byte[end - start];
                            System.arraycopy(geoTIFFBytes, start, chunk, 0, chunk.length);
                            return chunk;
                        }).toList()
        );
//        Dataset<Row> binaryFiles = spark.read().format("binaryFile")
//                .option("pathGlobFilter", "*.bin") // Filter for binary files
//                .load("path/to/binary/files");


        // Stop SparkSession
            spark.stop();
    }
}
