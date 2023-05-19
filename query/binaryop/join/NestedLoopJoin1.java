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
public class NestedLoopJoin1 extends BinaryOperation {

    Tuple curTuple1;
    Tuple nextTuple;
    

    public NestedLoopJoin1(Operation op1, Operation op2) throws Exception {
        super(op1, op2);
    }

    public NestedLoopJoin1(Operation op1, String sourceName1, Operation op2, String sourceName2) throws Exception {
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

        while (op1.hasNext() || curTuple1 != null) {
            if (curTuple1 == null) {
                curTuple1 = op1.next();
                op2.open();
            }

            while (op2.hasNext()) {
                Tuple curTuple2 = op2.next();
                if (Objects.equals(curTuple1.sourceTuples[tupleIndex1].record.getPrimaryKey(), curTuple2.sourceTuples[tupleIndex2].record.getPrimaryKey())) {
                    Tuple rec = new Tuple();
                    rec.addSources(curTuple1, curTuple2);

                    return rec;
                }

            }
            curTuple1 = null;
        }

        throw new Exception("No record after this point");

    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) {
            return true;
        }

        while (curTuple1 != null || op1.hasNext()) {
            if (curTuple1 == null) {
                curTuple1 = op1.next();
                op2.open();
            }
            while (op2.hasNext()) {
                Tuple curTuple2 = (Tuple) op2.next();
                if (Long.compare(curTuple1.sourceTuples[tupleIndex1].record.getPrimaryKey(), curTuple2.sourceTuples[tupleIndex2].record.getPrimaryKey())==0) {
                    nextTuple = new Tuple();
                    nextTuple.addSources(curTuple1, curTuple2);
                    return true;
                }

            }
            curTuple1 = null;
        }

        return false;
    }

    @Override
     public String toString(){
         return "Nested Loop Join";
     }
}
