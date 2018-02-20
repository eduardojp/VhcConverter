package vhcsearcher;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author eduardo
 */
public class Match {
    private static final int NO_OVERLAP = 0;
    private static final int CONTAINS = 1;
    private static final int IS_CONTAINED = 2;
    private static final int OVERLAPS_BEGIN = 3;
    private static final int OVERLAPS_END = 4;
    
    private Long sBegin;
    private Integer sDur;
    private Long dBegin;
    private Integer dDur;
    private String extRef;
    private boolean matched;

    public Long getsBegin() {
        return sBegin;
    }

    public void setsBegin(Long sBegin) {
        this.sBegin = sBegin;
    }

    public Integer getsDur() {
        return sDur;
    }

    public void setsDur(Integer sDur) {
        this.sDur = sDur;
    }

    public Long getdBegin() {
        return dBegin;
    }

    public void setdBegin(Long dBegin) {
        this.dBegin = dBegin;
    }

    public Integer getdDur() {
        return dDur;
    }

    public void setdDur(Integer dDur) {
        this.dDur = dDur;
    }

    public String getExtRef() {
        return extRef;
    }

    public void setExtRef(String extRef) {
        this.extRef = extRef;
    }

    @Override
    public String toString() {
        return "Match{" + "sBegin=" + sBegin + ", sDur=" + sDur + ", dBegin=" + dBegin + ", dDur=" + dDur + ", extRef=" + extRef + '}';
    }
    
    public String toStringDate() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");

        Date sBeginDate = new Date(sBegin);
        Date sEndDate = new Date(sBegin + sDur);
        Date dBeginDate = new Date(dBegin);
        Date dEndDate = new Date(dBegin + dDur);
        
        return dateFormat.format(sBeginDate) + " --> " + dateFormat.format(sEndDate) + "\n" +
            extRef + " " + dateFormat.format(dBeginDate) + " --> " + dateFormat.format(dEndDate);
            
    }
    
    public void markAsMatched(boolean overlaps) {
        this.matched |= overlaps;
    }
    
    public boolean isMatched() {
        return this.matched;
    }
    
    public static boolean compareMatch(Match searchMatch, Match templateMatch) {
        if(!searchMatch.extRef.equals(templateMatch.extRef)) {
            return false;
        }
        
        int result = compareLimits(searchMatch.getsBegin(), searchMatch.getsDur(), templateMatch.getsBegin(), templateMatch.getsDur());
        if(result == NO_OVERLAP) {
            return false;
        }
        
        int coveredTime, uncoveredTime, exceedingTime;
        long m1Begin = searchMatch.getdBegin();
        int m1Dur = searchMatch.getdDur();
        long m2Begin = templateMatch.getdBegin();
        int m2Dur = templateMatch.getdDur();
        long m1End = m1Begin + m1Dur;
        long m2End = m2Begin + m2Dur;
        
        switch(result) {
            case CONTAINS:
                uncoveredTime = 0;
                coveredTime = m2Dur;
                exceedingTime = (int) (m2Begin - m1Begin) + (int) (m1End - m2End);
                break;
            case IS_CONTAINED:
                coveredTime = m1Dur;
                uncoveredTime = (int) (m1Begin - m2Begin) + (int) (m2End - m1End);
                exceedingTime = 0;
                break;
            case OVERLAPS_BEGIN:
                uncoveredTime = (int) (m1Begin - m2Begin);
                coveredTime = (int) (m2End - m1Begin);
                exceedingTime = (int) (m1End - m2End);
                break;
            case OVERLAPS_END:
                exceedingTime = (int) (m1Begin - m2Begin);
                coveredTime = (int) (m1End - m2Begin);
                uncoveredTime = (int) (m2End - m1End);
                break;
            default:
                return false;
        }
        
        System.out.println("----------------------------");
        System.out.println(searchMatch.toStringDate() + "\nAND\n" + templateMatch.toStringDate());
        System.out.println(String.format("coveredTime: %dms (%.2f)", coveredTime, (double) coveredTime / (double) m2Dur));
        System.out.println(String.format("uncoveredTime: %dms (%.2f)", uncoveredTime, (double) uncoveredTime / (double) m2Dur));
        System.out.println(String.format("exceedingTime: %dms (%.2f)", exceedingTime, (double) exceedingTime / (double) m2Dur));
        
        return true;
    }
        
    private static int compareLimits(long m1Begin, int m1Dur, long m2Begin, int m2Dur) {
        long m1End = m1Begin + m1Dur;
        long m2End = m2Begin + m2Dur;

        //Origem
        //searchMatch contém templateMatch
        if(m1Begin <= m2Begin && m1End >= m2End) {
            return CONTAINS;
        }
        //searchMatch está contido em templateMatch
        else if(m1Begin >= m2Begin && m1End <= m2End) {
            return IS_CONTAINED;
        }
        //searchMatch compartilha início com o final de templateMatch
        else if(m1Begin >= m2Begin && m1Begin <= m2End && m1End >= m2End) {
            return OVERLAPS_BEGIN;
        }
        //searchMatch compartilha final com o início de templateMatch
        else if(m1Begin <= m2Begin && m1End >= m2Begin && m1End <= m2End) {
            return OVERLAPS_END;
        }
        //matches não se sobrepõem
        else {
            return NO_OVERLAP;
        }
    }
}