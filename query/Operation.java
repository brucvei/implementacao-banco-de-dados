/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query;

/**
 *
 * @author Sergio
 */
public abstract class Operation {
    
    protected String sources[];
    //boolean definedSources = false;
    
    Operation parentOperation;
    
    public String[] getSources() throws Exception{
    return sources;
    }
    
//    public boolean areSourcesDefined()
//    {
//    return definedSources;
//    }
     
    public  void open() throws Exception{
    //if (definedSources) return;
    feedSources();
    //definedSources = true;
    }
    
    public void setParentOperation(Operation op){
        parentOperation = op;
    }
    
    public Operation getParentOperation(){
        return parentOperation;
    }
    
    
    
    protected abstract void feedSources() throws Exception;
    public abstract Tuple next() throws Exception;
    public abstract boolean hasNext() throws Exception;
    public abstract void close() throws Exception;
    
    
    
}
