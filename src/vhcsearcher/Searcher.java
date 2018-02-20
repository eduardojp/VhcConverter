package vhcsearcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

/**
 *
 * @author eduardo
 */
public class Searcher {
    public static List<Match> searchMatches(String extRef, long begin, long end) throws IOException {
        List<Match> matchList = new LinkedList<>();

        List<String> command = new ArrayList<>();

        command.add("./vhc-searcher");
        command.add("-p");
        command.add("1");
        command.add("-s");
        command.add("-i");
        command.add(extRef);
        command.add("-b");
        command.add(begin + "");
        command.add("-e");
        command.add(end + "");
        
        command.add("-d");
        command.add("-i");
        command.add("BIS");
        
        command.add("-c");
        command.add("./config");

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();
        System.out.println("Processo executado");
        System.out.println(processBuilder.command());

        String inputLine;
        BufferedReader processReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        while((inputLine = processReader.readLine()) != null) {
            try {
                System.out.println(inputLine);

                StringTokenizer lineTokenizer = new StringTokenizer(inputLine);

                if(lineTokenizer.hasMoreElements()) {
                    switch(lineTokenizer.nextElement().toString()) {
                        case "MATCH":
                            Match match = new Match();
                            match.setsBegin(Long.parseLong(lineTokenizer.nextElement().toString()));
                            match.setsDur(Integer.parseInt(lineTokenizer.nextElement().toString()));
                            match.setExtRef(lineTokenizer.nextElement().toString());
                            match.setdBegin(Long.parseLong(lineTokenizer.nextElement().toString()));
                            match.setdDur(Integer.parseInt(lineTokenizer.nextElement().toString()));
                            matchList.add(match);
                            
                            System.out.println(match);
                            
                            break;
                    }
                }
            }
            catch(NumberFormatException ex) {
                ex.printStackTrace();
            }
        }

        try {
            if(process.waitFor() != 0) {
                System.out.println("REPORT VHCSEARCHER CRASHED ERROR");
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        return matchList;
    }
}

