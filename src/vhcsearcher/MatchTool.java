package vhcsearcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eduardo
 */
public class MatchTool {
    private final Database database;
    private final String templatePath;
    
    public MatchTool(String host, int port, String dbName, String user, String password, String templatePath) {
        this.templatePath = templatePath;
        this.database = null;
        //this.database = new Database(host, port, dbName, user, password);
    }
    
    public void run(String extRef, String extRef2, Long beginMillis, Long endMillis) {
        //database.insertNewHashing(extRef);
    }
    
    public void run() {
        HashMap<String, List<Match>> templateMatchListMap = new HashMap<>();
        List<Match> matchList;
        
        try(BufferedReader br = new BufferedReader(new FileReader(new File(templatePath)))) {
            String line;
            while((line = br.readLine()) != null) {
                System.out.println(line);
                
                StringTokenizer lineTokenizer = new StringTokenizer(line);

                if(lineTokenizer.hasMoreElements()) {
                    switch(lineTokenizer.nextElement().toString()) {
                        case "MATCH":
                            String extRef = lineTokenizer.nextElement().toString();
                            
                            Match match = new Match();
                            match.setsBegin(Long.parseLong(lineTokenizer.nextElement().toString()));
                            match.setsDur(Integer.parseInt(lineTokenizer.nextElement().toString()));
                            match.setExtRef(lineTokenizer.nextElement().toString());
                            match.setdBegin(Long.parseLong(lineTokenizer.nextElement().toString()));
                            match.setdDur(Integer.parseInt(lineTokenizer.nextElement().toString()));
                            
                            System.out.println(match);
                            
                            matchList = templateMatchListMap.get(extRef);
                            if(matchList == null) {
                                matchList = new LinkedList<>();
                                templateMatchListMap.put(extRef, matchList);
                            }
                            matchList.add(match);
                            
                            break;
                    }
                }
            }
            
            Set<String> keySet = templateMatchListMap.keySet();
            for(String key : keySet) {
                List<Match> templateMatchList = templateMatchListMap.get(key);
                List<Match> searchMatchList;

                for(Match templateMatch : templateMatchList) {
                    searchMatchList = Searcher.searchMatches(key, templateMatch.getsBegin(), templateMatch.getsBegin() + templateMatch.getsDur());

                    for(Match searchMatch : searchMatchList) {
                        boolean overlaps = Match.compareMatch(searchMatch, templateMatch);
                        searchMatch.markAsMatched(overlaps);
                        templateMatch.markAsMatched(overlaps);

                        //Falso positivo
                        if(!searchMatch.isMatched()) {
                            System.out.println("----------------------------");
                            System.out.println("Falso positivo: " + key + " " + searchMatch.toStringDate());
                        }
                    }

                    //Falso Negativo
                    if(!templateMatch.isMatched()) {
                        System.out.println("----------------------------");
                        System.out.println("Falso negativo: " + key + " " + templateMatch.toStringDate());
                    }
                }
            }
        }
        catch(FileNotFoundException ex) {
            Logger.getLogger(MatchTool.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IOException ex) {
            Logger.getLogger(MatchTool.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}