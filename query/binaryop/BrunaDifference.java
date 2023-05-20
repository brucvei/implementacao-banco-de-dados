package ibd.query.binaryop;

import ibd.query.Operation;
import ibd.query.Tuple;

public class BrunaDifference extends BinaryOperation {

    Tuple nextTuple;
    Tuple curTuple1;

    public BrunaDifference(Operation op1, Operation op2) throws Exception {
        super(op1, op2);
        tupleIndex1 = -1;
    }

    public BrunaDifference(Operation op1, String sourceName1, Operation op2, String sourceName2) throws Exception {
        super(op1, sourceName1, op2, sourceName2);
        this.tupleIndex1 = -1;
        this.tupleIndex2 = -1;
    }

    @Override
    public void open() throws Exception {
        getLeftOperation().open();
        System.out.println(getLeftSourceName());
        getRigthOperation().open();
        System.out.println(getRigthSourceName());
        super.open();

        super.findSourceIndex();
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
        return (nextTuple != null);
    }

    public boolean isPresent() throws Exception {
        boolean flag = false;
        while (op2.hasNext()) {
            Tuple curTuple2 = (Tuple) op2.next();
            if (curTuple1.sourceTuples[tupleIndex1].record.getPrimaryKey().equals(curTuple2.sourceTuples[tupleIndex2].record.getPrimaryKey())) {
                flag = true;
            }
        }
        return flag;
    }

    private Tuple join() throws Exception {
        while (curTuple1 != null || op1.hasNext()) {
            if (curTuple1 == null) {
                curTuple1 = op1.next();
                op2.open();
            }
            while (op2.hasNext()) {
                Tuple curTuple2 = (Tuple) op2.next();
                if (!isPresent()) {
                    Tuple tuple = new Tuple();
                    tuple.addSource(curTuple1);
                    return tuple;
                }
            }
            curTuple1 = null;
        }
        return null;
    }
}
