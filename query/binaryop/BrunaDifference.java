package ibd.query.binaryop;

import ibd.query.Operation;
import ibd.query.SourceTuple;
import ibd.query.Tuple;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.sourceop.PKFilterScan;

import static ibd.table.Utils.match;

public class BrunaDifference extends BinaryOperation {

	Tuple nextTuple;

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

//        this.filterScan = new PKFilterScan(getLeftSourceName(), op1, 0, 0);

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
			if (isntPresent()) {
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
        if (nextTuple != null)
            return true;

        while (){
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

	public boolean isntPresent() throws Exception {
		System.out.println(getLeftSourceName() + " - index " + tupleIndex1);
		System.out.println(getRigthSourceName() + " - index " + tupleIndex2);

		while (op2.hasNext()) {
			op2.next();
			findSourceIndex();
			if (match((long) tupleIndex1, (long) tupleIndex2, 0)) {
				return false;
			}
		}
		return true;
	}
}
