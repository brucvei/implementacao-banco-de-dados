/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.table.record.Record;
import ibd.table.Utils;
import ibd.index.ComparisonTypes;
import ibd.table.Table;

/**
 *
 * @author Sergio
 */
public class PKFilterScan extends TableScan {

    protected int comparisonType;
    protected Long value;
    
    protected int comparisonType2 = -1;
    protected Long value2;
    
    public PKFilterScan(String sourceName, Table table, int comparisonType, long value) {
        super(sourceName, table);
        this.comparisonType = comparisonType;
        this.value = value;
    }
    
    public PKFilterScan(String sourceName, Table table, int comparisonType, long value, int comparisonType2, long value2) {
        super(sourceName, table);
        this.comparisonType = comparisonType;
        this.value = value;
        this.comparisonType2 = comparisonType2;
        this.value2 = value2;
    }

    public int getComparisonType(){
        return comparisonType;
    }
    
    public long getValue(){
        return value;
    }
    
    public int getComparisonType2(){
        return comparisonType2;
    }
    
    public long getValue2(){
        return value2;
    }
    
    
    @Override
    public boolean match(Record rec){
        boolean ok = Utils.match(rec.getPrimaryKey(), value, comparisonType);
        if (!ok) return false;
        if (comparisonType2==-1) return true;
        return Utils.match(rec.getPrimaryKey(), value2, comparisonType2);
    }
    
    @Override
     public String toString(){
         return "["+sourceName+"]PK Filter Scan "+ComparisonTypes.getComparisonType(comparisonType)+" "+ value;
     }

}
