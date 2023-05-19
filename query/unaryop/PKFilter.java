/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.table.Utils;
import ibd.index.ComparisonTypes;
import ibd.query.Operation;
import ibd.query.Tuple;

/**
 *
 * @author Sergio
 */
public class PKFilter extends UnaryOperation{

    long value;
    int comparisonType;
    
    Long value2;
    int comparisonType2 = -1;
    
    
    String sourceName;
    
    int tupleIndex = -1;
    
    Tuple curTuple;
    Tuple nextTuple;
    
    public PKFilter(Operation op, String sourceName, int comparisonType, long value) throws Exception{
        super(op);
        this.sourceName = sourceName;
        this.comparisonType = comparisonType;
        this.value = value;
    }
    
    public PKFilter(Operation op, String sourceName, int comparisonType, long value, int comparisonType2, long value2) throws Exception{
        this(op, sourceName, comparisonType, value);
        this.comparisonType2 = comparisonType2;
        this.value2 = value2;
    }
    
    @Override
    public Operation getOperation() {
        return op;
    }

    @Override
    public void open() throws Exception {
        super.open(); 
        tupleIndex = -1;
        findSourceIndex();
            
        curTuple = null;
        nextTuple = null;
        
    }
    
    public String getSourceName(){
        return sourceName;
    }
    
    private void findSourceIndex() throws Exception{
    String[] sources = getSources();
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].equals(sourceName)){
                tupleIndex = i;
                break;
            }
            
        }
        if (tupleIndex==-1)
            throw new Exception("source not found");
    }

    @Override
    public Tuple next() throws Exception {
        
        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }
        
        while (op.hasNext()){
            Tuple tp = op.next();
            if (match(tp)){
                Tuple rec = new Tuple();
                rec.addSource(tp);
                //rec.primaryKey = tp.primaryKey;
                //rec.content = tp.content;
                return rec;
            }
            
        }
        throw new Exception("No more data");
    }

    @Override
    public boolean hasNext() throws Exception {
        if (nextTuple != null) 
            return true;
        
        
        while (op.hasNext()){
            Tuple tp = op.next();
            if (match(tp)){
                nextTuple = new Tuple();
                nextTuple.addSource(tp);
                //nextTuple.primaryKey = tp.primaryKey;
                //nextTuple.content = tp.content;
                return true;
            }
        }
        return false;
         
    }

     public boolean match(Tuple tp){
    boolean ok = Utils.match(tp.sourceTuples[tupleIndex].record.getPrimaryKey(),value, comparisonType);
    if (!ok) return false;
        if (comparisonType2==-1) return true;
        return Utils.match(tp.sourceTuples[tupleIndex].record.getPrimaryKey(),value2, comparisonType2);
    
    
    }
     
     
    @Override
     public String toString(){
         return "PK Filter ["+sourceName+"]"+ComparisonTypes.getComparisonType(comparisonType)+" "+ value;
     }
    
}
