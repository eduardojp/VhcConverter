package vhcsearcher;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eduardo
 */
public class Database {
    private String hostAddress;
    private int port;
    private String dbName;
    private String user;
    private String password;
    private int successfulConvs;
    private int failedConvs;
    
    public Database(String hostAddress, int port, String dbName, String user, String password) {
        this.hostAddress = hostAddress;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        
        this.successfulConvs = 0;
        this.failedConvs = 0;
        
        System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");

        try {
            Class.forName("org.postgresql.Driver");
        }
        catch(ClassNotFoundException e) {
            System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
            e.printStackTrace();
            return;
        }

        System.out.println("PostgreSQL JDBC Driver Registered!");

        Connection connection = null;

        try {
            connection = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);

        }
        catch(SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if(connection != null) {
            System.out.println("You made it, take control your database now!");
        }
        else {
            System.out.println("Failed to make connection!");
        }
    }

    public void insertNewHashing(String extRef) {
        String insertTableSQL = "INSERT INTO vsr_hashing(ext_ref,token,profile_id) VALUES(?,?,?)";
        System.out.println(">>> " + insertTableSQL);
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(insertTableSQL)) {
            
            ps.setString(1, extRef);
            ps.setString(2, "XXX");
            ps.setLong(3, 1);
            ps.executeUpdate();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    
    public void insert(byte[] bval, long id, int prefix) {
//        for(int i = 0; i < bval.length; i++) {
//            System.out.println(bval[i]);
//        }
        
        String insertTableSQL = "UPDATE vsr_hashing SET vhc_set_"+prefix+"=? WHERE id = " + id;
//        System.out.println(">>> " + insertTableSQL);
        
        try(//Connection con = cpds.getConnection();
            Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(insertTableSQL)) {
            
            ps.setBytes(1, bval);
            ps.executeUpdate();
            
            successfulConvs += bval.length / 12;
        }
        catch(SQLException e) {
            failedConvs += bval.length / 12;
            e.printStackTrace();
        }
    }
    
    public void insert2(byte[] bval, long id, int prefix, long timeRef) {
//        for(int i = 0; i < bval.length; i++) {
//            System.out.println(bval[i]);
//        }
        
        String insertTableSQL = "UPDATE vsr_hashing SET vhc_set_"+prefix+"=? WHERE id = " + id;
//        System.out.println(">>> " + insertTableSQL);
        
        try(//Connection con = cpds.getConnection();
            Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(insertTableSQL)) {
            
            ps.setBytes(1, bval);
            ps.executeUpdate();
            
            successfulConvs += bval.length / 12;
        }
        catch(SQLException e) {
            failedConvs += bval.length / 12;
            e.printStackTrace();
        }
    }
    
    List<String> loadChannels() {
        ArrayList<String> channelList = new ArrayList<>();
        String selectTableSQL = "SELECT ext_ref from VSR_HASHING GROUP BY ext_ref ORDER BY ext_ref";
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            while(rs.next()) {
                channelList.add(rs.getString("ext_ref"));
            }
        }
        catch(SQLException e) {
           e.printStackTrace();
        }
        
        return channelList;
    }
    
    public void loadVhcSetPrefix(String channel) {
        String selectTableSQL = "SELECT time_ref,vhc_set_0, vhc_set_1, vhc_set_2, vhc_set_3, vhc_set_4, vhc_set_5, vhc_set_6, vhc_set_7, vhc_set_8, vhc_set_9, vhc_set_10, vhc_set_11, vhc_set_12, vhc_set_13, vhc_set_14, vhc_set_15, vhc_set_16, vhc_set_17, vhc_set_18, vhc_set_19, vhc_set_20, vhc_set_21, vhc_set_22, vhc_set_23, vhc_set_24, vhc_set_25, vhc_set_26, vhc_set_27, vhc_set_28, vhc_set_29, vhc_set_30, vhc_set_31 FROM VSR_HASHING where ext_ref = '"+channel+"'";
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            int N_PREFIX_GROUPS = 32;
            int HASH_TIME_LENGTH = 4;
            int VHC_LENGTH = 8; //TODO: não está preparado para mais de 8 bytes
            
            while(rs.next()) {
                long timeRef = rs.getLong("time_ref");
                
                for(int i = 0; i < N_PREFIX_GROUPS; i++) {
                    byte[] vhcSet = rs.getBytes("vhc_set_" + i);
                    int index = 0;
                    
                    if(vhcSet == null) {
                        continue;
                    }
                    if(vhcSet.length % (HASH_TIME_LENGTH + VHC_LENGTH) != 0) {
                        System.out.println("WARNING: vhc_set_" + i + " não é multiplo de (HASH_TIME length + VHC length) = " + HASH_TIME_LENGTH + VHC_LENGTH);
                    }
                    
                    while(true) {
                        if(index + HASH_TIME_LENGTH + VHC_LENGTH > vhcSet.length) {
                            break;
                        }
                        
                        int hashTime =
                            (vhcSet[index] & 0xFF) << 24 |
                            (vhcSet[index+1] & 0xFF) << 16 |
                            (vhcSet[index+2] & 0xFF) << 8 |
                            (vhcSet[index+3] & 0xFF);

                        long vhc =
                            ((long)(vhcSet[index+4] & 0xFF)) << 56 |
                            ((long)(vhcSet[index+5] & 0xFF)) << 48 |
                            ((long)(vhcSet[index+6] & 0xFF)) << 40 |
                            ((long)(vhcSet[index+7] & 0xFF)) << 32 |
                            ((long)(vhcSet[index+8] & 0xFF)) << 24 |
                            ((long)(vhcSet[index+9] & 0xFF)) << 16 |
                            ((long)(vhcSet[index+10] & 0xFF)) << 8 |
                            ((long)(vhcSet[index+11] & 0xFF));
                        
                        System.out.println("t: " + (timeRef+hashTime));
                        System.out.println("v: " + vhc);
                        
                        index += HASH_TIME_LENGTH + VHC_LENGTH;
                    }
                }
            }
        }
        catch(SQLException ex) {
           ex.printStackTrace();
        }
    }
    
    public void loadVhcSetPrefixTimeIndex(String channel) {
        String selectTableSQL = "SELECT time_ref,vhc_set_0, vhc_set_1, vhc_set_2, vhc_set_3, vhc_set_4, vhc_set_5, vhc_set_6, vhc_set_7, vhc_set_8, vhc_set_9, vhc_set_10, vhc_set_11, vhc_set_12, vhc_set_13, vhc_set_14, vhc_set_15, vhc_set_16, vhc_set_17, vhc_set_18, vhc_set_19, vhc_set_20, vhc_set_21, vhc_set_22, vhc_set_23, vhc_set_24, vhc_set_25, vhc_set_26, vhc_set_27, vhc_set_28, vhc_set_29, vhc_set_30, vhc_set_31 FROM VSR_HASHING where ext_ref = '"+channel+"'";
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            int N_PREFIX_GROUPS = 32;
            int TIME_INDEX_LENGTH = 2;
            int VHC_LENGTH = 8; //TODO: não está preparado para mais de 8 bytes
            
            while(rs.next()) {
                long timeRef = rs.getLong("time_ref");
                System.out.println("time_ref: " + timeRef + "\n");
                
                for(int i = 0; i < N_PREFIX_GROUPS; i++) {
                    byte[] vhcSet = rs.getBytes("vhc_set_" + i);
                    int index = 0;
                    
                    if(vhcSet == null) {
                        continue;
                    }
                    if(vhcSet.length % (TIME_INDEX_LENGTH + VHC_LENGTH) != 0) {
                        System.out.println("WARNING: vhc_set_" + i + " não é multiplo de (HASH_TIME length + VHC length) = " + TIME_INDEX_LENGTH + VHC_LENGTH);
                    }
                    
                    while(true) {
                        if(index + TIME_INDEX_LENGTH + VHC_LENGTH > vhcSet.length) {
                            break;
                        }
                        
                        int timeIndex =
                            (vhcSet[index] & 0xFF) << 8 |
                            (vhcSet[index+1] & 0xFF);

                        long vhc =
                            ((long)(vhcSet[index+2] & 0xFF)) << 56 |
                            ((long)(vhcSet[index+3] & 0xFF)) << 48 |
                            ((long)(vhcSet[index+4] & 0xFF)) << 40 |
                            ((long)(vhcSet[index+5] & 0xFF)) << 32 |
                            ((long)(vhcSet[index+6] & 0xFF)) << 24 |
                            ((long)(vhcSet[index+7] & 0xFF)) << 16 |
                            ((long)(vhcSet[index+8] & 0xFF)) << 8 |
                            ((long)(vhcSet[index+9] & 0xFF));
                        
                        System.out.println("t: " + (timeRef + timeIndex*1000));
                        System.out.println("v: " + vhc);
                        
                        index += TIME_INDEX_LENGTH + VHC_LENGTH;
                    }
                }
                
                if(true) break;
            }
        }
        catch(SQLException ex) {
           ex.printStackTrace();
        }
    }

    public void loadVhcInsertHashingPrefixTimeIndex(String channel, int currentChannelNumber, int totalChannelNumber, int displayCount) {
        String selectTableSQL = "SELECT id,time_ref FROM VSR_HASHING where ext_ref = '"+channel+"' order by id";
        LinkedList<Long> hashingIds = new LinkedList<>();
        HashMap<Long, Long> hashingMap = new HashMap<>();
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            while(rs.next()) {
                long id = rs.getLong("id");
                long timeRef = rs.getLong("time_ref");
                
                hashingIds.add(id);
                hashingMap.put(id, timeRef);
            }
        }
        catch(SQLException e) {
           e.printStackTrace();
        }

        //------------------------------------------
        
        ArrayList<LinkedList<Byte>> byteListVector = new ArrayList<>();
        for(int i = 0; i < 32; i++) {
            byteListVector.add(new LinkedList<>());
        }
        
        int nVhcs = 0;
        int currentHashingIdNumber = 1;
        
        for(long hashingId : hashingIds) {
            
            //selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE ext_ref = '"+channel+"' AND hashing_id = " + refId + " order by absolute_time";
            selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE hashing_id = " + hashingId + " order by absolute_time";
//            System.out.println(">>> " + selectTableSQL);
        
            long timeRef = hashingMap.get(hashingId);
            long newTimeRef = 0;
            boolean firstTimeRef = true;
            int vhcIndex = 0;
            
            try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
                PreparedStatement ps = con.prepareStatement(selectTableSQL); 
                ResultSet rs = ps.executeQuery()) {
                
                while(rs.next()) {
                    long vhc = rs.getLong("vhc");
                    
                    //O novo timeRef será o tempo absolute do primeiro vhc
                    if(firstTimeRef) {
                        newTimeRef = rs.getLong("absolute_time");
                        firstTimeRef = false;
//                        System.out.println(timeRef + " / " + newTimeRef);
//                        System.exit(0);
                    }
                    
                    int prefix = (int)(vhc >> 59 & 31);
                    LinkedList<Byte> byteList = byteListVector.get(prefix);

//System.out.println(vhc + " added to " + prefix);

                    byteList.add((byte)((vhcIndex >> 8) & 0xFF));
                    byteList.add((byte)(vhcIndex & 0xFF));

                    byteList.add((byte)((vhc >> 56) & 0xFF));
                    byteList.add((byte)((vhc >> 48) & 0xFF));
                    byteList.add((byte)((vhc >> 40) & 0xFF));
                    byteList.add((byte)((vhc >> 32) & 0xFF));
                    byteList.add((byte)((vhc >> 24) & 0xFF));
                    byteList.add((byte)((vhc >> 16) & 0xFF));
                    byteList.add((byte)((vhc >> 8) & 0xFF));
                    byteList.add((byte)(vhc & 0xFF));
                    
                    vhcIndex++;
                }

                StringBuilder sb = new StringBuilder(1000);
                sb.append("UPDATE vsr_hashing SET time_ref=?");
                for(int prefix = 0; prefix < 32; prefix++) {
                    sb.append(", vhc_set_");
                    sb.append(prefix);
                    sb.append("=?");
                }
                sb.append(" WHERE id = ");
                sb.append(hashingId);
                String insertTableSQL = sb.toString();

//                System.out.println(">>> " + insertTableSQL);
//                System.out.println(">>> " + insertTableSQL);
//                System.exit(0);
        
                int vhcsToConvert = 0;
                
                try(
                    Connection con2 = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
                    PreparedStatement ps2 = con2.prepareStatement(insertTableSQL)) {
                    
                    ps2.setLong(1, newTimeRef);
                    
                    for(int prefix = 0; prefix < 32; prefix++) {
                        LinkedList<Byte> byteList = byteListVector.get(prefix);
                        byte[] bytea = new byte[byteList.size()];
                        int i = 0;
                        for(byte b : byteList) {
                            bytea[i++] = b;
                        }
                        
                        ps2.setBytes(prefix+2, bytea);
                        
                        vhcsToConvert += byteList.size()/10;
                        nVhcs += vhcsToConvert;
                        byteList.clear();
                    }

                    ps2.executeUpdate();
                    
                    //System.out.println(nVhcs + " " + displayCount);
                    if(nVhcs > displayCount) {
                        System.out.println(
                            String.format("%d/%d %s %d %d %.2f%%",
                                currentChannelNumber, totalChannelNumber, channel, successfulConvs, failedConvs,
                                100 * ((double) currentHashingIdNumber / (double) hashingIds.size()))
                        );
                        nVhcs = displayCount % nVhcs;
                    }

                    successfulConvs += vhcsToConvert;
                }
                catch(SQLException e) {
                    failedConvs += vhcsToConvert;
                    e.printStackTrace();
                }
            }
            catch(SQLException e) {
               e.printStackTrace();
            }
            
            currentHashingIdNumber++;
        }
    }
    
    public void loadVhcInsertHashingPrefix(String channel, int currentChannelNumber, int totalChannelNumber, int displayCount) {
        String selectTableSQL = "SELECT id,time_ref FROM VSR_HASHING where ext_ref = '"+channel+"' order by id";
        LinkedList<Long> hashingIds = new LinkedList<>();
        HashMap<Long, Long> hashingMap = new HashMap<>();
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            while(rs.next()) {
                long id = rs.getLong("id");
                long timeRef = rs.getLong("time_ref");
                
                hashingIds.add(id);
                hashingMap.put(id, timeRef);
            }
        }
        catch(SQLException e) {
           e.printStackTrace();
        }

        //------------------------------------------
        
        ArrayList<LinkedList<Byte>> byteListVector = new ArrayList<>();
        for(int i = 0; i < 32; i++) {
            byteListVector.add(new LinkedList<>());
        }
        
        int nVhcs = 0;
        int currentHashingIdNumber = 1;
        
        for(long refId : hashingIds) {
            
            //selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE ext_ref = '"+channel+"' AND hashing_id = " + refId + " order by absolute_time";
            selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE hashing_id = " + refId + " order by absolute_time";
//            System.out.println(">>> " + selectTableSQL);
        
            long timeRef = hashingMap.get(refId);
            
            try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
                PreparedStatement ps = con.prepareStatement(selectTableSQL); 
                ResultSet rs = ps.executeQuery()) {
                
                while(rs.next()) {
                    int time = (int)(rs.getLong("absolute_time") - timeRef);
                    long vhc = rs.getLong("vhc");
                    
                    int prefix = (int)(vhc >> 59 & 31);
                    LinkedList<Byte> byteList = byteListVector.get(prefix);

//System.out.println(vhc + " added to " + prefix);

                    byteList.add((byte)((time >> 24) & 0xFF));
                    byteList.add((byte)((time >> 16) & 0xFF));
                    byteList.add((byte)((time >> 8) & 0xFF));
                    byteList.add((byte)(time & 0xFF));

                    byteList.add((byte)((vhc >> 56) & 0xFF));
                    byteList.add((byte)((vhc >> 48) & 0xFF));
                    byteList.add((byte)((vhc >> 40) & 0xFF));
                    byteList.add((byte)((vhc >> 32) & 0xFF));
                    byteList.add((byte)((vhc >> 24) & 0xFF));
                    byteList.add((byte)((vhc >> 16) & 0xFF));
                    byteList.add((byte)((vhc >> 8) & 0xFF));
                    byteList.add((byte)(vhc & 0xFF));
                }
                
                for(int prefix = 0; prefix < 32; prefix++) {
                    LinkedList<Byte> byteList = byteListVector.get(prefix);
                    byte[] bytea = new byte[byteList.size()];
                    int i = 0;
                    for(byte b : byteList) {
                        bytea[i++] = b;
                    }

                    nVhcs += byteList.size()/12;
//                    System.out.println(prefix + ": " + byteList.size()/12);
                    insert(bytea, refId, prefix);
                    byteList.clear();
                    
//                    System.out.println(nVhcs + " " + displayCount);
                    if(nVhcs > displayCount) {
                        System.out.println(
                            String.format("%d/%d %s %d %d %.2f%%",
                                currentChannelNumber, totalChannelNumber, channel, successfulConvs, failedConvs,
                                100 * ((double) currentHashingIdNumber / (double) hashingIds.size()))
                        );
                        nVhcs = displayCount % nVhcs;
                    }
                }
            }
            catch(SQLException e) {
               e.printStackTrace();
            }
            
            currentHashingIdNumber++;
        }
    }
    
    public void loadVhcInsertFile(String channel, int currentChannelNumber, int totalChannelNumber, int displayCount) {
        String selectTableSQL = "SELECT id,time_ref FROM VSR_HASHING where ext_ref = '"+channel+"' order by id";
        LinkedList<Long> hashingIds = new LinkedList<>();
        HashMap<Long, Long> hashingMap = new HashMap<>();
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            while(rs.next()) {
                long id = rs.getLong("id");
                long timeRef = rs.getLong("time_ref");
                
                hashingIds.add(id);
                hashingMap.put(id, timeRef);
            }
        }
        catch(SQLException e) {
           e.printStackTrace();
        }

        //------------------------------------------
        
        ArrayList<LinkedList<Vhc>> vhcListVector = new ArrayList<>();
        for(int i = 0; i < 32; i++) {
            vhcListVector.add(new LinkedList<>());
        }
        
        int vhcCount = 0;
        int currentHashingIdNumber = 1;
        
        for(long refId : hashingIds) {
            
            //selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE ext_ref = '"+channel+"' AND hashing_id = " + refId + " order by absolute_time";
            selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE hashing_id = " + refId + " order by absolute_time";
//            System.out.println(">>> " + selectTableSQL);
        
            long timeRef = hashingMap.get(refId);
            
            try(BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream("/home/eduardo/NetBeansProjects/VhcConverter/data2/"+channel+"/"+refId+".vhcset"));
                Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
                PreparedStatement ps = con.prepareStatement(selectTableSQL); 
                ResultSet rs = ps.executeQuery()) {
                
                while(rs.next()) {
                    int time = (int)(rs.getLong("absolute_time") - timeRef);
                    long vhc = rs.getLong("vhc");
                    
                    int prefix = (int)(vhc >> 59 & 31);
                    LinkedList<Vhc> vhcList = vhcListVector.get(prefix);
                    vhcList.add(new Vhc(time, vhc));
                }
                
                for(int prefix = 0; prefix < 32; prefix++) {
                    LinkedList<Vhc> vhcList = vhcListVector.get(prefix);
                    Collections.sort(vhcList);
                    
                    byte[] bytea = new byte[vhcList.size() * 12];
                    int nVhcsPerPrefix = vhcList.size();
                            
                    vhcCount += nVhcsPerPrefix;
                   
                    int i = 0;
                    for(Vhc vhc : vhcList) {
                        for(byte b : vhc.bytes) {
                            bytea[i++] = b;    
                        }
                    }

//                    System.out.println(prefix + ": " + byteList.size()/12);
                    output.write(bytea);
                    vhcList.clear();
                    
//                    System.out.println(nVhcs + " " + displayCount);
                    if(vhcCount > displayCount) {
                        System.out.println(
                            String.format("%d/%d %s %d %d %.2f%%",
                                currentChannelNumber, totalChannelNumber, channel, successfulConvs, failedConvs,
                                100 * ((double) currentHashingIdNumber / (double) hashingIds.size()))
                        );
                        vhcCount = displayCount % vhcCount;
                    }
                }
            }
            catch(SQLException e) {
               e.printStackTrace();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            currentHashingIdNumber++;
        }
    }
    
    public void loadVhcCheckStep(String channel, int currentChannelNumber, int totalChannelNumber, int displayCount) {
        String selectTableSQL = "SELECT id,time_ref FROM VSR_HASHING where ext_ref = '"+channel+"' order by id";
        LinkedList<Long> hashingIds = new LinkedList<>();
        HashMap<Long, Long> hashingMap = new HashMap<>();
        
        try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
            PreparedStatement ps = con.prepareStatement(selectTableSQL); 
            ResultSet rs = ps.executeQuery()) {
            
            while(rs.next()) {
                long id = rs.getLong("id");
                long timeRef = rs.getLong("time_ref");
                
                hashingIds.add(id);
                hashingMap.put(id, timeRef);
            }
        }
        catch(SQLException e) {
           e.printStackTrace();
        }

        //------------------------------------------
        
        ArrayList<LinkedList<Vhc>> vhcListVector = new ArrayList<>();
        for(int i = 0; i < 32; i++) {
            vhcListVector.add(new LinkedList<>());
        }
        
        int vhcCount = 0;
        int currentHashingIdNumber = 1;
        
        for(long refId : hashingIds) {
            
            //selectTableSQL = "SELECT vhc,absolute_time FROM vsr_vhc WHERE ext_ref = '"+channel+"' AND hashing_id = " + refId + " order by absolute_time";
            selectTableSQL = "SELECT absolute_time FROM vsr_vhc WHERE hashing_id = " + refId + " order by absolute_time";
//            System.out.println(">>> " + selectTableSQL);
        
            long timeRef = hashingMap.get(refId);
            boolean firstVhc = true;
            long lastVhcTime = 0;
            
            try(Connection con = DriverManager.getConnection("jdbc:postgresql://"+hostAddress+":"+port+"/"+dbName, user, password);
                PreparedStatement ps = con.prepareStatement(selectTableSQL); 
                ResultSet rs = ps.executeQuery()) {
                
                while(rs.next()) {
                    long time = rs.getLong("absolute_time");
                    vhcCount++;
                    
                    if(firstVhc) {
                        firstVhc = false;
                    }
                    else {
                        if(time - lastVhcTime != 1000) {
                            System.out.printf("%d - %d = %d\n", time, lastVhcTime, time - lastVhcTime);
                        }
                    }
                    
                    lastVhcTime = time;
                }
            }
            catch(Exception e) {
               e.printStackTrace();
            }
            
            if(vhcCount > displayCount) {
                System.out.println(
                    String.format("%d/%d %s %d %d %.2f%%",
                        currentChannelNumber, totalChannelNumber, channel, successfulConvs, failedConvs,
                        100 * ((double) currentHashingIdNumber / (double) hashingIds.size()))
                );
                vhcCount = displayCount % vhcCount;
            }
            
            currentHashingIdNumber++;
        }
    }
}