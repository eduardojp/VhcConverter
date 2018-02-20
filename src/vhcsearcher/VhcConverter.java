package vhcsearcher;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @author eduardo
 */
public class VhcConverter {
    private final Database database;
    
    public VhcConverter(String host, int port, String dbName, String user, String password) {
        this.database = new Database(host, port, dbName, user, password);
    }
    
    public void run(List<String> channelList, int displayCount) {
        if(channelList == null) {
            channelList = database.loadChannels();
        }
        
        int currentChannelNumber = 1;
        for(String channel : channelList) {
//            database.loadVhcInsertHashingPrefix(channel, currentChannelNumber, channelList.size(), displayCount);
//            database.loadVhcInsertFile(channel, currentChannelNumber, channelList.size(), displayCount);
//            database.loadVhcSetPrefix(channel);

            database.loadVhcInsertHashingPrefixTimeIndex(channel, currentChannelNumber, channelList.size(), displayCount);
//            database.loadVhcSetPrefixTimeIndex(channel);
//            database.loadVhcCheckStep(channel, currentChannelNumber, currentChannelNumber, displayCount);
            
            currentChannelNumber++;
        }
    }
    
    public static void run(String channel) throws FileNotFoundException, IOException {
        File folder = new File("/home/eduardo/NetBeansProjects/VhcConverter/data/" + channel);
        String[] fileNames = folder.list();
        byte[] buffer = new byte[10000000];
        
        //Arrays.sort
        
        Arrays.sort(fileNames, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                int v1 = Integer.parseInt(s1.split("\\.")[0]);
                int v2 = Integer.parseInt(s2.split("\\.")[0]);
                
                return v1 - v2;
            }
        });
        
        for(String fileName : fileNames) {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream("/home/eduardo/NetBeansProjects/VhcConverter/data/"+channel+"/"+fileName));
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream("/home/eduardo/NetBeansProjects/VhcConverter/data2/"+channel+"/"+fileName));
            
            System.out.println("fileName: " + fileName);
            
            while(true) {
                int b1 = in.read(); if(b1 == -1) break;
                int b2 = in.read(); if(b2 == -1) break;
                int b3 = in.read(); if(b3 == -1) break;
                int b4 = in.read(); if(b4 == -1) break;
                
                int nVhcs =
                    (b1 & 0xFF) << 24 |
                    (b2 & 0xFF) << 16 |
                    (b3 & 0xFF) << 8 |
                    (b4 & 0xFF);
                
                System.out.println("nVhcs: " + nVhcs);
                
                int ret = in.read(buffer, 0, nVhcs*12);
                out.write(buffer, 0, nVhcs*12);
                
                if(ret == -1) break;
                
//                System.exit(0);
            }
        }
    }
}