/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop.join;

import ibd.query.Operation;
import ibd.query.Tuple;
import ibd.query.binaryop.BinaryOperation;
import java.util.Objects;

/**
 *
 * @author SergioI
 */
public class NestedLoopJoin extends BinaryOperation {

    Tuple curTuple1;
    Tuple nextTuple;
    

    public NestedLoopJoin(Operation op1, Operation op2) throws Exception {
        super(op1, op2);
    }

    public NestedLoopJoin(Operation op1, String sourceName1, Operation op2, String sourceName2) throws Exception {
        super(op1, sourceName1, op2, sourceName2);
    }
    

    @Override
    public void open() throws Exception {

        super.open();
        curTuple1 = null;
        nextTuple = null;

    }

    

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        return join();

    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) {
            return true;
        }
        nextTuple = join();
        

        return (nextTuple!=null);
    }
    
    private Tuple join() throws Exception{
    
    while (curTuple1 != null || op1.hasNext()) {
            if (curTuple1 == null) {
                curTuple1 = op1.next();
                op2.open();
            }
            while (op2.hasNext()) {
                Tuple curTuple2 = (Tuple) op2.next();
                if (Long.compare(curTuple1.sourceTuples[tupleIndex1].record.getPrimaryKey(), curTuple2.sourceTuples[tupleIndex2].record.getPrimaryKey())==0) {
                    Tuple tuple = new Tuple();
                    tuple.addSources(curTuple1, curTuple2);
                    return tuple;
                }

            }
            curTuple1 = null;
        }
    return null;
    }

    @Override
     public String toString(){
         return "Nested Loop Join";
     }
}
