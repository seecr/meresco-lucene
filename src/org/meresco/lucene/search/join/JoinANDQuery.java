package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;

public class JoinANDQuery implements RelationalQuery {
    private RelationalQuery second;
    private RelationalQuery first;
    private String secondKeyName;
    private String firstKeyName;

    public JoinANDQuery(String firstKeyName, String secondKeyName, RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
        this.firstKeyName = firstKeyName;
        this.secondKeyName = secondKeyName;
    }

    @Override
    public Result execute() {
        this.first.prepareCollectKeys(this.firstKeyName);
        Result result = this.first.execute();
        BitSet bits = result.getBitSet();
        System.out.println("bits: " + bits);
        this.second.addFilter(bits, this.secondKeyName);
        return this.second.execute();
    }

    @Override
    public void addFilter(BitSet keyFilter, String keyName) {
        // TODO Auto-generated method stub
    }

    @Override
    public void prepareCollectKeys(String keyName) {
        // TODO Auto-generated method stub

    }
}