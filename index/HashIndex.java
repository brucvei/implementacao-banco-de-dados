/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.index;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Sergio
 */
public class HashIndex extends IndexManagement implements Iterable<IndexRecord> {

    public Hashtable<Long, IndexRecord> index = new Hashtable();

    public HashIndex(String folder, String name, int pageSize, boolean recreate) throws IOException {
        super(folder, name, pageSize, recreate);
    }

    @Override
    public void clear() {
        super.clear();
        index.clear();
    }

    protected void addEntry(Long primaryKey, IndexRecord ir) {
        index.put(primaryKey, ir);
    }

    protected boolean isEmpty() {
        return index.isEmpty();
    }

    protected void removeEntry(Long primaryKey) {
        index.remove(primaryKey);
    }

    protected void updateEntry(Long primaryKey) {
    }

    @Override
    public IndexRecord getEntry(Long primaryKey) {
        return index.get(primaryKey);
    }

    @Override
    public List<IndexRecord> getEntries(Long primaryKey, int comparisonType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRecordsAmount() {
        return index.size();
    }

    @Override
    public Iterator iterator() {
        return new HashIndexIterator(this);
        //return index.values().iterator();
    }

    class HashIndexIterator implements Iterator {

        Iterator it;

        public HashIndexIterator(HashIndex index) {
            this.it = index.index.values().iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {
            return it.next();
        }

    }

}
