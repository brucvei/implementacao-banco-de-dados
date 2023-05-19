package ibd.table;

import ibd.query.Operation;
import ibd.query.Tuple;
import ibd.query.binaryop.BinaryOperation;

public class BrunaDifference extends BinaryOperation {

    public BrunaDifference(Operation op1, Operation op2) throws Exception {
        super(op1, op2);
    }

    public BrunaDifference(Operation op1, String sourceName1, Operation op2, String sourceName2) throws Exception {
        super(op1, sourceName1, op2, sourceName2);
    }

    public void open() throws Exception {

    }

    public Tuple next() throws Exception {
        return null;
    }

    public boolean hasNext() throws Exception {
        return false;
    }
}
