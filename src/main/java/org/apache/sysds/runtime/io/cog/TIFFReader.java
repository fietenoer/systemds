package org.apache.sysds.runtime.io.cog;

import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TIFFReader extends COGHeader{
    private String filename;
    private byte[] headerFile;
    private COGHeader newCOG;
    private ByteOrder byteOrder;
    public  TIFFReader(){
        this.newCOG = new COGHeader();
        this.headerFile = new byte[8];
    }

    public TIFFReader(String filename){
        this.filename = filename;
    }

    public void parseTiff() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            // Read header (8 bytes)

            file.readFully(headerFile);

            // endianness

            if(headerFile[0] == 'I' && headerFile[1] == 'I'){
                this.byteOrder = ByteOrder.LITTLE_ENDIAN;
                this.newCOG.setLittleEndian(true);
            }
            else{
                this.byteOrder = ByteOrder.BIG_ENDIAN;
                this.newCOG.setLittleEndian(false);
            }
            ByteBuffer buffer = ByteBuffer.wrap(headerFile).order(byteOrder);

            //TIFF magic number
            int magicNumber = buffer.getShort(2) & 0xFFFF;
            if (magicNumber != 42) {
                throw new IllegalArgumentException("Not a valid TIFF file!");
            }

            // Offset to first IFD
            int ifdOffset = buffer.getInt(4);

            // Parse IFDs
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
