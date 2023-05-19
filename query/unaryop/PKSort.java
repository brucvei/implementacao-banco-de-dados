/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.query.Operation;
import ibd.query.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 *
 * @author Sergio
 */
public class PKSort extends UnaryOperation {

    String sourceName;
    int tupleIndex = -1;
    
    Tuple nextTuple = null;
    int currentIndex = -1;
    ArrayList<Tuple> tt;
    
    //boolean opened = false;

    public PKSort(Operation op, String sourceName) throws Exception {
        super(op);
        this.sourceName = sourceName;
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
    public void open() throws Exception {
        super.open();
        
        tupleIndex = -1;
        findSourceIndex();
        
        nextTuple = null;
        currentIndex = -1;
        
        //if (opened) return;
        
        tt = new ArrayList<>();
        //op.open();
        while (op.hasNext()){
            Tuple tuple = (Tuple)op.next();
            tt.add(tuple);
            
        }
        
        Collections.sort(tt, new TupleComparator() );
        
        //opened = true;
        
    }

    
    public class TupleComparator implements Comparator<Tuple> {
  
    @Override
    public int compare(Tuple tt1, Tuple tt2) {
       return Long.compare(tt1.sourceTuples[tupleIndex].record.getPrimaryKey(), tt2.sourceTuples[tupleIndex].record.getPrimaryKey());
    }
}

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        currentIndex++;
        if (currentIndex == tt.size()) {
            throw new Exception("No records after this point");
        }

        
        return tt.get(currentIndex);

        
    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) 
            return true;
        
        if (currentIndex+1 >= tt.size())
            return false;
        
        currentIndex++;
        nextTuple = tt.get(currentIndex);
        return true;
    }

    @Override
     public String toString(){
         return "PK Sort";
     }
    
}
