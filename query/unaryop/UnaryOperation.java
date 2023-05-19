/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.query.Operation;

/**
 *
 * @author Sergio
 */
public abstract class UnaryOperation extends Operation {

    protected Operation op;
    
    public UnaryOperation(Operation op) throws Exception {
        setOperation(op);
    }

    public final void setOperation(Operation op) {
        this.op = op;
        op.setParentOperation(this);
    }
    
    public Operation getOperation() {
        return op;
    }

    @Override
    public void open() throws Exception {
        op.open();
        super.open();
    }
    
    @Override
    public void close() throws Exception{
        op.close();
    }

    @Override
    protected void feedSources() throws Exception {
        String s[] = op.getSources();
        sources = new String[s.length];
        for (int i = 0; i < s.length; i++) {
            sources[i] = s[i];
        }

    }

    
}
