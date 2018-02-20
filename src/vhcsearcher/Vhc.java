package vhcsearcher;

/**
 *
 * @author eduardo
 */
public class Vhc implements Comparable<Vhc> {
    public int time;
    public long vhc;
    public byte[] bytes;
    
    public Vhc(int time, long vhc) {
        this.time = time;
        this.vhc = vhc;
        this.bytes = new byte[12];
        
        bytes[0] = ((byte)((time >> 24) & 0xFF));
        bytes[1] = ((byte)((time >> 16) & 0xFF));
        bytes[2] = ((byte)((time >> 8) & 0xFF));
        bytes[3] = ((byte)(time & 0xFF));

        bytes[4] = ((byte)((vhc >> 56) & 0xFF));
        bytes[5] = ((byte)((vhc >> 48) & 0xFF));
        bytes[6] = ((byte)((vhc >> 40) & 0xFF));
        bytes[7] = ((byte)((vhc >> 32) & 0xFF));
        bytes[8] = ((byte)((vhc >> 24) & 0xFF));
        bytes[9] = ((byte)((vhc >> 16) & 0xFF));
        bytes[10] = ((byte)((vhc >> 8) & 0xFF));
        bytes[11] = ((byte)(vhc & 0xFF));
    }
    
    public Vhc(int time, long vhc, int nada) {
        this.time = time;
        this.vhc = vhc;
        this.bytes = new byte[10];
        
        bytes[0] = ((byte)((time >> 8) & 0xFF));
        bytes[1] = ((byte)(time & 0xFF));

        bytes[2] = ((byte)((vhc >> 56) & 0xFF));
        bytes[3] = ((byte)((vhc >> 48) & 0xFF));
        bytes[4] = ((byte)((vhc >> 40) & 0xFF));
        bytes[5] = ((byte)((vhc >> 32) & 0xFF));
        bytes[6] = ((byte)((vhc >> 24) & 0xFF));
        bytes[7] = ((byte)((vhc >> 16) & 0xFF));
        bytes[8] = ((byte)((vhc >> 8) & 0xFF));
        bytes[9] = ((byte)(vhc & 0xFF));
    }

    @Override
    public int compareTo(Vhc o) {
        return Long.compareUnsigned(this.vhc, o.vhc);
    }
}