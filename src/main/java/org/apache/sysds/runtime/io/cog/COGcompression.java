package org.apache.sysds.runtime.io.cog;

public class COGcompression {
    private boolean isLZW;
    private IFDTagDictionary compression;
    private IFDTagDictionary stripbytecounts;
    private IFDTagDictionary stripOffsets;
     COGcompression(){
         this.isLZW = false;
     }
     COGcompression(IFDTagDictionary ifd){
         this.compression = ifd.Compression;
         this.stripbytecounts = ifd.StripByteCounts;
         this.stripOffsets = ifd.StripOffsets;
         if(compression.getValue() == 5){
             this.isLZW = true;
             decompress();
         }
     }
     private void decompress(){ // performs the custom decompression

     }
}
