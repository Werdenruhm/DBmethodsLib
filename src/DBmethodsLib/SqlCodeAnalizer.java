package DBmethodsLib;

import CommonLib.CodeAnalizer;
import CommonLib.Common;
import java.util.ArrayList;

/**
 *
 * 
 */
public class SqlCodeAnalizer implements CodeAnalizer<SqlCodeAnalizer.SqlNonCodePlace> 
{

    public final static SqlCodeAnalizer instance;
    static { instance = new SqlCodeAnalizer(); }
    public final static SqlNonCodePlace[] allNonCode 
            = new SqlNonCodePlace[] { SqlNonCodePlace.multilineComments, SqlNonCodePlace.singleLineComments, SqlNonCodePlace.stringConst, SqlNonCodePlace.doublequotaId };
    @Override
    public SqlNonCodePlace[] allNonCode() {
        return allNonCode;
    }
    public ArrayList<SqlPart> getParts(String sqlText)
    {            
        ArrayList<SqlPart> result = new ArrayList<>();
        parceCode(sqlText.toCharArray(), null, (type, begIx, endIxExcl) -> {
            SqlPart pr = new SqlPart(); pr.text = sqlText.substring(begIx, endIxExcl); pr.type = type; pr.begIx = begIx; pr.endIxExcl = endIxExcl;                        
            result.add(pr);
        });
        return result;
    }
    public boolean[] getCodeMask(String sqlText)
    {            
        boolean[] result = new boolean[sqlText.length()];
        parceCode(sqlText.toCharArray(), (placeType, chrN) -> {
            result[chrN] = placeType == null;
        }, null);
        return result;
    }
    public static class ReplacePair { public final char[] searchValue; public final char[] replaceValue; 
        public ReplacePair(String searchValue, String replaceValue) { this.searchValue = searchValue.toCharArray(); this.replaceValue = replaceValue.toCharArray(); } 
    }
    private final static SqlNonCodePlace[] codeplace = new SqlNonCodePlace[] { null };
    private final static SqlNonCodePlace[] codeplaceandcomments = new SqlNonCodePlace[] { null, SqlCodeAnalizer.SqlNonCodePlace.singleLineComments, SqlCodeAnalizer.SqlNonCodePlace.multilineComments};
    public String replaceInCode(String sqlText, ReplacePair[] toReplace, Common.Action2<ReplacePair, SqlNonCodePlace> replaced)
    {            
        return replace(sqlText, toReplace, replaced, codeplace, false);
    }
    ReplacePair[] tabreplpair = new SqlCodeAnalizer.ReplacePair[] { new SqlCodeAnalizer.ReplacePair("\n", "\n    ") };
    public String tab(String sqlText)
    {            
        return replace(sqlText, tabreplpair, null, codeplaceandcomments, true);
    }
    public String tab(String sqlText, int tabs)
    {            
        if (tabs == 0)
            return sqlText;
        if (tabs == 1)
            return tab(sqlText);
        String tabsS = "";
        for (int n = 0; n < Math.abs(tabs); n++)
            tabsS += "    ";
        return replace(sqlText, new SqlCodeAnalizer.ReplacePair[] { new SqlCodeAnalizer.ReplacePair("\n" + (tabs < 0 ? tabsS : "" ), "\n" + (tabs > 0 ? tabsS : "")) }, null, codeplaceandcomments, true);
    }
    public String replaceInCodeAndComments(String sqlText, ReplacePair[] toReplace, Common.Action2<ReplacePair, SqlNonCodePlace> replaced, boolean alsoReplaceInSqlNonCodePlaceSeparators)
    {            
        return replace(sqlText, toReplace, replaced, codeplaceandcomments, alsoReplaceInSqlNonCodePlaceSeparators);
    }
    public String replace(String sqlText, ReplacePair[] toReplace, Common.Action2<ReplacePair, SqlNonCodePlace> replaced, SqlNonCodePlace[] replaceIn, boolean alsoReplaceInSqlNonCodePlaceSeparators)
    {
        for (SqlNonCodePlace placeType : replaceIn)
            for(ReplacePair rpl : toReplace)
                if (placeType != null && !alsoReplaceInSqlNonCodePlaceSeparators 
                        && (Common.ArrayContainsArray(rpl.replaceValue, placeType.end) || Common.ArrayContainsArray(placeType.end, rpl.replaceValue))
                )
                    throw new IllegalArgumentException("Try to replace with '" + String.valueOf(rpl.replaceValue) + "' in block " + placeType.toString()
                            + ". Replacement value contains terminating character (" + String.valueOf(placeType.end) + ") for this type of block, witch will lead to syntax errors! "
                            + "For unsafe replacement set alsoReplaceInSqlNonCodePlaceSeparators = true.");
            
        char[] sqlTextA = sqlText.toCharArray();
        StringBuilder result = new StringBuilder();        
        parceCode(sqlTextA, null, (placeType, begIx, endIxExcl) -> {
            if (Common.ArrayContains(replaceIn, placeType))
            {
                int goodBegIx = placeType != null && !alsoReplaceInSqlNonCodePlaceSeparators ? begIx + placeType.beg.length : begIx;
                int goodEndIxExcl =  placeType != null && !alsoReplaceInSqlNonCodePlaceSeparators ? endIxExcl - placeType.end.length : endIxExcl;
                        
                if (placeType != null && !alsoReplaceInSqlNonCodePlaceSeparators)
                    result.append(placeType.beg);
                
                int nextBeg = goodBegIx;
                for(int chrN = goodBegIx; chrN < goodEndIxExcl; chrN++)
                    for(ReplacePair rpl : toReplace)
                        if ((chrN + rpl.searchValue.length) <= goodEndIxExcl && Common.ArrayContainsArrayAt(sqlTextA, chrN, rpl.searchValue))
                        {
                            if (nextBeg < chrN)                                
                                result.append(sqlTextA, nextBeg, chrN - nextBeg);
                            result.append(rpl.replaceValue);
                            nextBeg = chrN + rpl.searchValue.length;
                            chrN = nextBeg - 1;
                            if (replaced != null)
                                replaced.call(rpl, placeType);
                            break;
                        }
                
                if (nextBeg < goodEndIxExcl)                                
                    result.append(sqlTextA, nextBeg, goodEndIxExcl - nextBeg);
                
                if (placeType != null && !alsoReplaceInSqlNonCodePlaceSeparators)
                    result.append(placeType.end);
            }
            else
                result.append(sqlTextA, begIx, endIxExcl - begIx);            
        });
        return result.toString();
    }
    public static class SqlPart { public String text; public SqlNonCodePlace type; public int begIx; public int endIxExcl; }
    public static enum SqlNonCodePlace implements CodeAnalizer.NonCodePlace { 
        multilineComments("/*", "*/"), 
        singleLineComments("--", "\n"), 
        stringConst("'", "'"), 
        doublequotaId("\"", "\"")
        ;  
        final char[] beg; final char[] end;
        SqlNonCodePlace(String beg, String end) { this.beg = beg.toCharArray(); this.end = end.toCharArray(); }  
        @Override public char[] beg() { return beg; }
        @Override public char[] end() { return end; }
    }
}
