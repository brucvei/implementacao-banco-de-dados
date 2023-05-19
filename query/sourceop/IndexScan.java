/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.table.record.Record;
import ibd.query.Tuple;
import ibd.table.Table;
import java.util.Iterator;

/**
 *
 * @author Sergio
 */
public class IndexScan extends SourceOperation {

    Table table;
    Iterator<Record> it;

    public IndexScan(String sourceName, Table table) {
        super(sourceName);
        this.table = table;
    }

    @Override
    public void open() throws Exception {

        it = table.iterator();
    }

    @Override
    public Tuple next() throws Exception {

        Record rec = it.next();
        Tuple tuple = new Tuple();
        tuple.addSource(sources[0], rec);
        return tuple;
    }

    @Override
    public boolean hasNext() throws Exception {
        return it.hasNext();

    }

    @Override
    public void close() throws Exception {
        
    }
    
    @Override
     public String toString(){
         return "["+sourceName+"] Index Scan";
     }

}
