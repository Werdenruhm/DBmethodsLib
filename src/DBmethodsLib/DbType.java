package DBmethodsLib;

public enum DbType {
BIT {@Override public int toInt(){ return -7; } @Override public String toPgName(){ return "boolean"; } },
    /*
TINYINT {@Override public int toInt(){ return -6; } },
SMALLINT {@Override public int toInt(){ return 5; } },
    */
INTEGER {@Override public int toInt(){ return 4; } @Override public String toPgName(){ return "integer"; } },
BIGINT {@Override public int toInt(){ return -5; } @Override public String toPgName(){ return "bigint"; } },
    /*
FLOAT {@Override public int toInt(){ return 6; } },
REAL {@Override public int toInt(){ return 7; } },
DOUBLE {@Override public int toInt(){ return 8; } },
NUMERIC {@Override public int toInt(){ return 2; } },
DECIMAL {@Override public int toInt(){ return 3; } },
CHAR {@Override public int toInt(){ return 1; } },
*/
VARCHAR {@Override public int toInt(){ return 12; } @Override public String toPgName(){ return "varchar"; } },
JSON {@Override public int toInt(){ return 12; } @Override public String toPgName(){ return "json"; } },
/*
LONGVARCHAR {@Override public int toInt(){ return -1; } },
DATE {@Override public int toInt(){ return 91; } },
TIME {@Override public int toInt(){ return 92; } },
*/
TIMESTAMP {@Override public int toInt(){ return 93; } @Override public String toPgName(){ return "timestamp"; } },
/*
TIMESTAMPNS {@Override public int toInt(){ return -100; } },
TIMESTAMPTZ {@Override public int toInt(){ return -101; } },
TIMESTAMPLTZ {@Override public int toInt(){ return -102; } },
INTERVALYM {@Override public int toInt(){ return -103; } },
INTERVALDS {@Override public int toInt(){ return -104; } },
BINARY {@Override public int toInt(){ return -2; } },
VARBINARY {@Override public int toInt(){ return -3; } },
LONGVARBINARY {@Override public int toInt(){ return -4; } },
ROWID {@Override public int toInt(){ return -8; } },
CURSOR {@Override public int toInt(){ return -10; } },
*/
BLOB {@Override public int toInt(){ return 2004; } @Override public String toPgName(){ return "bytea"; } },
/*
CLOB {@Override public int toInt(){ return 2005; } },
BFILE {@Override public int toInt(){ return -13; } },
STRUCT {@Override public int toInt(){ return 2002; } },
*/
ARRAY {@Override public int toInt(){ return 2003; } @Override public String toPgName(){ return "anyarray"; } @Override public String toPgOutName(){ return "text[]"; } },
/*
REF {@Override public int toInt(){ return 2006; } },
NCHAR {@Override public int toInt(){ return -15; } },
NCLOB {@Override public int toInt(){ return 2011; } },
NVARCHAR {@Override public int toInt(){ return -9; } },
LONGNVARCHAR {@Override public int toInt(){ return -16; } },
SQLXML {@Override public int toInt(){ return 2009; } },
OPAQUE {@Override public int toInt(){ return 2007; } },
JAVA_STRUCT {@Override public int toInt(){ return 2008; } },
JAVA_OBJECT {@Override public int toInt(){ return 2000; } },
PLSQL_INDEX_TABLE {@Override public int toInt(){ return -14; } },
BINARY_FLOAT {@Override public int toInt(){ return 100; } },
BINARY_DOUBLE {@Override public int toInt(){ return 101; } },
NULL {@Override public int toInt(){ return 0; } },
*/
NUMBER {@Override public int toInt(){ return 2; } @Override public String toPgName(){ return "numeric"; } },
/*
RAW {@Override public int toInt(){ return -2; } },
OTHER {@Override public int toInt(){ return 1111; } },
FIXED_CHAR {@Override public int toInt(){ return 999; } },
DATALINK {@Override public int toInt(){ return 70; } },
*/
BOOLEAN {@Override public int toInt(){ return 16; } @Override public String toPgName(){ return "boolean"; } }

;
public abstract int toInt(); 
public abstract String toPgName(); 
public String toPgOutName() { return toPgName(); } 
}
