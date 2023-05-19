package ibd.query.binaryop;

import ibd.query.Operation;
import ibd.query.SourceTuple;
import ibd.query.Tuple;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.sourceop.PKFilterScan;

import static ibd.table.Utils.match;

public class BrunaDifference extends BinaryOperation {

    Tuple nextTuple;
    Tuple curTuple1;

    public BrunaDifference(Operation op1, Operation op2) throws Exception {
        super(op1, op2);
    }

    public BrunaDifference(Operation op1, String sourceName1, Operation op2, String sourceName2) throws Exception {
        super(op1, sourceName1, op2, sourceName2);
    }

    @Override
    public void open() throws Exception {

        System.out.println("OPEN");
        getLeftOperation().open();
        System.out.println(getLeftSourceName());
        getRigthOperation().open();
        System.out.println(getRigthSourceName());
        super.open();

        super.findSourceIndex();
    }

    @Override
    public Tuple next() throws Exception {
        System.out.println("NEXT");

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        while (op1.hasNext()) {
            Tuple tp = op1.next();
            findSourceIndex();
            if (!isPresent()) {
                Tuple rec = new Tuple();
                rec.addSource(tp);
//                    rec.primaryKey = tp.primaryKey;
                //rec.content = tp.content;
                return rec;
            }
        }
        throw new Exception("No more data");
    }

    @Override
    public boolean hasNext() throws Exception {
        System.out.println("HASNEXT");
        if (nextTuple != null) {
            return true;
        }
        nextTuple = join();
        return (nextTuple != null);
    }

    public boolean isPresent() throws Exception {
        System.out.println(getLeftSourceName() + " - index " + tupleIndex1);
        System.out.println(getRigthSourceName() + " - index " + tupleIndex2);

        while (op2.hasNext()) {
            op2.next();
            findSourceIndex();
            if (match((long) tupleIndex1, (long) tupleIndex2, 0)) {
                return true;
            }
        }
        return false;
    }

    private Tuple join() throws Exception {

        while (curTuple1 != null || op1.hasNext()) {
            if (curTuple1 == null) {
                curTuple1 = op1.next();
                op2.open();
            }
            while (op2.hasNext()) {
                Tuple curTuple2 = (Tuple) op2.next();
                if (Long.compare(curTuple1.sourceTuples[tupleIndex1].record.getPrimaryKey(), curTuple2.sourceTuples[tupleIndex2].record.getPrimaryKey()) == 0) {
                    Tuple tuple = new Tuple();
                    tuple.addSources(curTuple1, curTuple2);
                    return tuple;
                }

            }
            curTuple1 = null;
        }
        return null;
    }
}
