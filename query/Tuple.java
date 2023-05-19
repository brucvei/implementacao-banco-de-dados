/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query;

import ibd.table.record.AbstractRecord;
import ibd.table.record.Record;

/**
 *
 * @author Sergio
 */
public class Tuple 
{
    
    //public long primaryKey;
    public SourceTuple sourceTuples[];
    
    
    //public void addSource(String source, long primaryKey, String content){
    public void addSource(String source, AbstractRecord record){
    SourceTuple sourceTuple = new SourceTuple();
    sourceTuple.source = source;
    sourceTuple.record = record;
    //sourceTuple.primaryKey = primaryKey;    
    //sourceTuple.content = content;
    sourceTuples = new SourceTuple[1];
    sourceTuples[0] = sourceTuple;
    }
    
    public void setSources(SourceTuple sourceTuples[]){
    this.sourceTuples = sourceTuples;
    }
    
    public void addSource(Tuple t){
    sourceTuples = new SourceTuple[t.sourceTuples.length];
    int count = 0;
        for (int i = 0; i < t.sourceTuples.length; i++) {
            sourceTuples[count] = t.sourceTuples[i];
            count++;
        }
    
    }
    
    public void addSources(Tuple t2){
    sourceTuples = new SourceTuple[this.sourceTuples.length+t2.sourceTuples.length];
    int count = 0;
        for (int i = 0; i < this.sourceTuples.length; i++) {
            sourceTuples[count] = this.sourceTuples[i];
            count++;
        }
        for (int i = 0; i < t2.sourceTuples.length; i++) {
            sourceTuples[count] = t2.sourceTuples[i];
            count++;
        }
    
    }

    public void addSources(Tuple t1, Tuple t2){
    sourceTuples = new SourceTuple[t1.sourceTuples.length+t2.sourceTuples.length];
    int count = 0;
        for (int i = 0; i < t1.sourceTuples.length; i++) {
            sourceTuples[count] = t1.sourceTuples[i];
            count++;
        }
        for (int i = 0; i < t2.sourceTuples.length; i++) {
            sourceTuples[count] = t2.sourceTuples[i];
            count++;
        }
    
    }
    
    
    
    public int size(){
        return sourceTuples.length * Record.RECORD_SIZE;
    }
    
    
    public int findSourceIndexByName(String name){
        String str = new String();
        for (int i = 0; i < sourceTuples.length; i++) {
            SourceTuple st = sourceTuples[i];
            if (st.source.equals(name))
                return i;
            
        }
        return -1;
    }
    
    @Override
    public String toString(){
        String str = new String();
        for (int i = 0; i < sourceTuples.length; i++) {
            SourceTuple st = sourceTuples[i];
            str+=st.record.getPrimaryKey()+":"+st.record.getContent().trim()+", ";
            
        }
        return str;
    }
    
    
    public static void main(String[] args) throws CloneNotSupportedException {
        Tuple t = new Tuple();
        t.addSource("source0", null);
        t.addSource("source1", null);
        Tuple t1 = (Tuple)t.clone();
        for (int i = 0; i < t1.sourceTuples.length; i++) {
            SourceTuple st = t1.sourceTuples[i];
            System.out.println(st.source);
            
        }
    }
}
