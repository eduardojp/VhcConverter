package vhcsearcher;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author eduardo
 */
public class Main {
    public static void main(String[] args) {
        
//        try {
//            VhcConverter.run("AEE");
//            System.exit(0);
//        }
//        catch(Exception ex) {
//            ex.printStackTrace();
//        }
        
        final HashMap<String, ArrayList<String>> params = new HashMap<>();

        ArrayList<String> options = null;
        for(int i = 0; i < args.length; i++) {
            final String a = args[i];
            
            if(a.charAt(0) == '-') {
                if(a.length() < 2) {
                    System.err.println("Error at argument " + a);
                    return;
                }

                options = params.get(a.substring(1));
                
                if(options == null) {
                    options = new ArrayList<>();
                    params.put(a.substring(1), options);
                }
            }
            else if(options != null) {
                options.add(a);
            }
            else {
                System.err.println("Illegal parameter usage");
                return;
            }
        }

        //-h DBHOST -p DBPORT -d DBNAME -u DBUSRNAME -w DBUSERPASS -c DISPLAYCOUNT

        ArrayList<String> channelList = null;
        String dbHost;
        Integer port;
        String dbName;
        String dbUserName;
        String dbUserPass;
        int displayCount = 3600;

        try {
            options = params.get("h");
            if(options != null) {
                dbHost = options.get(0);
            }
            else {
                throw new Exception("Especifique -h DBHOST");
            }

            options = params.get("p");
            if(options != null) {
                port = Integer.parseInt(options.get(0));
            }
            else {
                throw new Exception("Especifique -p DBPORT");
            }
            
            options = params.get("d");
            if(options != null) {
                dbName = options.get(0);
            }
            else {
                throw new Exception("Especifique -d DBNAME");
            }
            
            options = params.get("u");
            if(options != null) {
                dbUserName = options.get(0);
            }
            else {
                throw new Exception("Especifique -u DBUSRNAME");
            }
            
            options = params.get("w");
            if(options != null) {
                dbUserPass = options.get(0);
            }
            else {
                throw new Exception("Especifique -w DBUSERPASS");
            }
            
            options = params.get("c");
            if(options != null) {
                displayCount = Integer.parseInt(options.get(0));
            }
            
            channelList = params.get("e");
        }
        catch(Exception ex) {
            System.err.println(ex.getMessage());
            return;
        }
        
        VhcConverter vhcConverter = new VhcConverter(
            dbHost,
            port,
            dbName,
            dbUserName,
            dbUserPass
        );
        vhcConverter.run(channelList, displayCount);
    }
}