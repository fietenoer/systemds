package org.apache.sysds.runtime.io.cog;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import scala.collection.JavaConverters;

public class Decompression {
    private static final int CLEAR_CODE = 256;
    private static final int END_CODE = 257;
    private static final int INITIAL_DICT_SIZE = 258;

    public static byte[] decompress(byte[] compressedData) throws IllegalArgumentException, IOException {
        // Initialize dictionary with single-byte entries (0-255)
        Map<Integer, String> dictionary = new HashMap<>();
        for (int i = 0; i < 256; i++) {
            dictionary.put(i, String.valueOf((char) i));
        }

        int dictionarySize = INITIAL_DICT_SIZE; // Start after CLEAR_CODE and END_CODE
        int codeSize = 9; // Initial code size for GeoTIFF LZW
        int maxCodeSize = 12; // GeoTIFF typically uses up to 12-bit codes

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Read the first code
        int bitOffset = 0;
        int currentCode = getNextCode(compressedData, codeSize, bitOffset);
        bitOffset += codeSize;

        // Handle the first code
        if (currentCode == END_CODE) {
            return output.toByteArray(); // Empty result
        } else if (currentCode == CLEAR_CODE) {
            // Reset the dictionary and parameters
            dictionary = resetDictionary();
            dictionarySize = INITIAL_DICT_SIZE;
            codeSize = 9;
            currentCode = getNextCode(compressedData, codeSize, bitOffset);
            bitOffset += codeSize;
        }

        // Validate the first code
        if (currentCode < 0 || currentCode >= dictionarySize) {
            throw new IllegalArgumentException("Invalid first LZW code: " + currentCode);
        }

        // Write the initial string to output
        String currentString = dictionary.get(currentCode);
        output.write(currentString.getBytes());

        while (bitOffset <= compressedData.length * 8) {
            // Read the next code
            int nextCode = getNextCode(compressedData, codeSize, bitOffset);
            bitOffset += codeSize;

            if (nextCode == END_CODE) {
                break; // End of data
            }

            if (nextCode == CLEAR_CODE) {
                // Reset the dictionary and parameters
                dictionary = resetDictionary();
                dictionarySize = INITIAL_DICT_SIZE;
                codeSize = 9;
                // Get the next valid code
                currentCode = getNextCode(compressedData, codeSize, bitOffset);
                bitOffset += codeSize;

                // Validate and process
                if (currentCode < 0 || currentCode >= dictionarySize) {
                    throw new IllegalArgumentException("Invalid LZW code after CLEAR_CODE: " + currentCode);
                }
                currentString = dictionary.get(currentCode);
                output.write(currentString.getBytes());
                continue;
            }

            // Handle regular codes
            String nextString;
            if (dictionary.containsKey(nextCode)) {
                nextString = dictionary.get(nextCode);
            } else if (nextCode == dictionarySize) {
                nextString = currentString + currentString.charAt(0);
            } else {
                throw new IllegalArgumentException("Invalid LZW code: " + nextCode);
            }

            // Write the decoded string to output
            output.write(nextString.getBytes());

            // Add new entry to the dictionary
            if (dictionarySize < (1 << maxCodeSize)) {
                dictionary.put(dictionarySize++, currentString + nextString.charAt(0));

                // Increase code size if dictionary size exceeds current limit
                if (dictionarySize == (1 << codeSize) && codeSize < maxCodeSize) {
                    codeSize++;
                }
            }

            // Update the current string
            currentString = nextString;
        }

        return output.toByteArray();
    }

    private static Map<Integer, String> resetDictionary() {
        // Reinitialize the dictionary with single-byte entries (0-255)
        Map<Integer, String> dictionary = new HashMap<>();
        for (int i = 0; i < 256; i++) {
            dictionary.put(i, String.valueOf((char) i));
        }
        return dictionary;
    }


    private static int getNextCode(byte[] data, int codeSize, int bitOffset) {
        int code = 0;

        // Validate that we have enough bits to read
        if (bitOffset + codeSize > data.length * 8) {
            throw new IllegalArgumentException("Insufficient data to read a full code of size " + codeSize + " bits.");
        }

        // Read 'codeSize' bits
        for (int i = 0; i < codeSize; i++) {
            int byteIndex = (bitOffset + i) / 8;
            int bitIndex = (bitOffset + i) % 8;

            // Ensure byteIndex is within bounds
            if (byteIndex >= data.length) {
                throw new IllegalArgumentException("Attempted to read beyond compressed data bounds.");
            }

            int bit = (data[byteIndex] >> (7 - bitIndex)) & 1;
            code = (code << 1) | bit;
        }

        return code-1;
    }

//    public byte[] createJob(byte[] geoTIFFBytes) {
//        // Initialize Spark Configuration and Session
//        SparkConf conf = new SparkConf()
//                .setAppName("GeoTIFF Decompression")
//                .setMaster("local[*]") // Use local mode with all cores
//                .set("spark.executor.memory", "4g");
//
//        SparkSession spark = SparkSession
//                .builder()
//                .config(conf)
//                .getOrCreate();
//
//        System.out.println("Hey");
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//        // Split byte array into chunks for parallel processing
//        int chunkSize = 1024; // Adjust chunk size based on your needs
//        int numChunks = (int) Math.ceil((double) geoTIFFBytes.length / chunkSize);
//
//        // Create Java list of chunks
//        List<byte[]> chunks = new ArrayList<>();
//        for (int i = 0; i < numChunks; i++) {
//            int start = i * chunkSize;
//            int end = Math.min(start + chunkSize, geoTIFFBytes.length);
//            byte[] chunk = new byte[end - start];
//            System.arraycopy(geoTIFFBytes, start, chunk, 0, chunk.length);
//            chunks.add(chunk);
//        }
//
//        // Use JavaSparkContext to parallelize the data
//        JavaRDD<byte[]> rdd = jsc.parallelize(chunks, numChunks);
//
//        // Apply LZW decompression on each chunk
//        JavaRDD<byte[]> decompressedRDD = rdd.mapPartitions(partition -> {
//            List<byte[]> decompressedChunks = new ArrayList<>();
//            while (partition.hasNext()) {
//                byte[] chunk = partition.next();
//                Decompression decompressor = new Decompression();
//                decompressedChunks.add(decompressor.decompress(chunk));
//            }
//            return decompressedChunks.iterator();
//        });
//
//        // Collect and combine the decompressed results
//        List<byte[]> decompressedChunks = decompressedRDD.collect();
//        byte[] finalDecompressedData = combineChunks(decompressedChunks);
//        spark.stop();
//        return finalDecompressedData;
//
//    }
//
//    private byte[] combineChunks(List<byte[]> chunks) {
//        int totalSize = chunks.stream().mapToInt(chunk -> chunk.length).sum();
//        byte[] combined = new byte[totalSize];
//        int offset = 0;
//        for (byte[] chunk : chunks) {
//            System.arraycopy(chunk, 0, combined, offset, chunk.length);
//            offset += chunk.length;
//        }
//        return combined;
//    }

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
}

