package org.apache.sysds.runtime.io.cog;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import scala.collection.JavaConverters;

public class Decompression {
    private static final int CLEAR_CODE = 256;
    private static final int END_CODE = 257;
    private static final int INITIAL_DICT_SIZE = 258;
    public byte[] decompress(byte[] compressedData) throws IOException {
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

        return decompressedData;
    }

    public byte[] createJob(byte[] geoTIFFBytes) {
        // Initialize Spark Configuration and Session
        SparkConf conf = new SparkConf()
                .setAppName("GeoTIFF Decompression")
                .setMaster("local[*]") // Use local mode with all cores
                .set("spark.executor.memory", "4g");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Split byte array into chunks for parallel processing
        int chunkSize = 1024; // Adjust chunk size based on your needs
        int numChunks = (int) Math.ceil((double) geoTIFFBytes.length / chunkSize);

        // Create Java list of chunks
        List<byte[]> chunks = new ArrayList<>();
        for (int i = 0; i < numChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, geoTIFFBytes.length);
            byte[] chunk = new byte[end - start];
            System.arraycopy(geoTIFFBytes, start, chunk, 0, chunk.length);
            chunks.add(chunk);
        }

        // Use JavaSparkContext to parallelize the data
        JavaRDD<byte[]> rdd = jsc.parallelize(chunks, numChunks);

        // Apply LZW decompression on each chunk
        JavaRDD<byte[]> decompressedRDD = rdd.mapPartitions(partition -> {
            List<byte[]> decompressedChunks = new ArrayList<>();
            while (partition.hasNext()) {
                byte[] chunk = partition.next();
                Decompression decompressor = new Decompression();
                decompressedChunks.add(decompressor.decompress(chunk));
            }
            return decompressedChunks.iterator();
        });

        // Collect and combine the decompressed results
        List<byte[]> decompressedChunks = decompressedRDD.collect();
        byte[] finalDecompressedData = combineChunks(decompressedChunks);
        spark.stop();
        return finalDecompressedData;

    }

    private byte[] combineChunks(List<byte[]> chunks) {
        int totalSize = chunks.stream().mapToInt(chunk -> chunk.length).sum();
        byte[] combined = new byte[totalSize];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, combined, offset, chunk.length);
            offset += chunk.length;
        }
        return combined;
    }

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

