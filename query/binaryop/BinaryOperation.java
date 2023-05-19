/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop;

import ibd.query.Operation;

/**
 *
 * @author Sergio
 */
public abstract class BinaryOperation extends Operation {

    protected Operation op1;
    protected Operation op2; 
    
    protected String sourceName1;
    protected String sourceName2;
    protected int tupleIndex1 = -1;
    protected int tupleIndex2 = -1;

    public BinaryOperation(Operation op1, Operation op2) throws Exception {
        setLeftOperation(op1);
        setRightOperation(op2);
    }
    
    public BinaryOperation(Operation op1, String sourceName1, Operation op2, String sourceName2) throws Exception {
        this(op1, op2);
        this.sourceName1 = sourceName1;
        this.sourceName2 = sourceName2;
    }

    
    
    @Override
    public void open() throws Exception {
        getLeftOperation().open();
        getRigthOperation().open();
        super.open();
        findSourceIndex();
    }

    void findSourceIndex() throws Exception {
        if (sourceName1 == null) {
            tupleIndex1 = 0;
        } else {
            String[] sources1 = op1.getSources();
            for (int i = 0; i < sources1.length; i++) {
                if (sources1[i].equals(sourceName1)) {
                    tupleIndex1 = i;
                    break;
                }

            }
        }
        if (sourceName2 == null) {
            tupleIndex2 = 0;
        } else {
            if (tupleIndex1 == -1) {
                throw new Exception("source not found");
            }

            String[] sources2 = op2.getSources();
            for (int i = 0; i < sources2.length; i++) {
                if (sources2[i].equals(sourceName2)) {
                    tupleIndex2 = i;
                    break;
                }

            }
            if (tupleIndex2 == -1) {
                throw new Exception("source not found");
            }
        }
    }
    
    @Override
    protected void feedSources() throws Exception {
        
        String left[] = getLeftOperation().getSources();
        String right[] = getRigthOperation().getSources();
        sources = new String[left.length + right.length];
        int count = 0;
        for (int i = 0; i < left.length; i++) {
            sources[count] = left[i];
            count++;
        }
        for (int i = 0; i < right.length; i++) {
            sources[count] = right[i];
            count++;
        }

    }
    
    public final void setLeftOperation(Operation op){
        op1 = op;
        op.setParentOperation(this);
    }
    
    public final void setRightOperation(Operation op){
        op2 = op;
        op.setParentOperation(this);
    }

    public Operation getLeftOperation() {
        return op1;
    }

    public Operation getRigthOperation() {
        return op2;
    }
    
    public String getLeftSourceName() {
        return sourceName1;
    }

    public String getRigthSourceName() {
        return sourceName2;
    }
    
    @Override
    public void close() throws Exception{
        op1.close();
        op2.close();
    }
    
    @Override
     public String toString(){
         return "Binary Operation";
     }

}
