/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.table.record.Record;
import ibd.query.Tuple;
import ibd.table.block.Block;
import ibd.table.Table;
import java.util.Iterator;

/**
 *
 * @author Sergio
 */
public class InverseTableScan extends SourceOperation {

    public int currentBlock;
    
    public Block currentBlock_;
    
    public int lastBlock;
    public int currentRecord;
    public Table table;

    Tuple nextTuple = null;
    boolean reachedEnd = false;
    
    Iterator<Record> iterator;

    public InverseTableScan(String sourceName, Table table) {
        super(sourceName);
        this.table = table;
    }

    @Override
    public void open() throws Exception {

        super.open();
        currentBlock = table.getLargestBlock();
        currentBlock_ = table.getBlock(currentBlock);
        iterator = currentBlock_.iterator();
        
        lastBlock = 0;
        currentRecord = -1;
        nextTuple = null;
        reachedEnd = false;
    }

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }
        
        if (reachedEnd)
            throw new Exception("No records after this point");

        while (iterator.hasNext() || currentBlock_.prev_block_id!=-1) {
            if (iterator.hasNext()){
                Record record = iterator.next();
                if (match(record)){
                    Tuple tuple = new Tuple();
                    tuple.addSource(sources[0], record);
                    return tuple;
                }
                else continue;
            }
            currentBlock_ = table.getBlock(currentBlock_.next_block_id);
            iterator = currentBlock_.iterator();
            }

        
        reachedEnd = true;

        throw new Exception("No records after this point");
    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) {
            return true;
        }
        
        if (reachedEnd)
            return false;

        
        while (iterator.hasNext() || currentBlock_.prev_block_id!=-1) {
            if (iterator.hasNext()){
                Record record = iterator.next();
                if (match(record)){
                    nextTuple = new Tuple();
                    nextTuple.addSource(sources[0], record);
                    //System.out.println("found "+nextTuple+" in block "+currentBlock_);
                    return true;
                }
                else continue;
            }
            currentBlock_ = table.getBlock(currentBlock_.prev_block_id);
            iterator = currentBlock_.iterator();
            }

        
        reachedEnd = true;
    return false;
    }

    
    public boolean match(Record rec){
    return true;
    }

    @Override
    public void close() throws Exception {
        
    }
    
    @Override
     public String toString(){
         return "["+sourceName+"] Table Scan";
     }

}
