package dk.aau.cs.sw10.swod;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by alex on 5/9/14.
 */
public class Pair<L,R> {

    private final L left;
    private final R right;

    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() { return left; }
    public R getRight() { return right; }

    @Override
    public int hashCode() { return left.hashCode() ^ right.hashCode(); }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof Pair)) return false;
        Pair pairo = (Pair) o;
        return this.left.equals(pairo.getLeft()) &&
                this.right.equals(pairo.getRight());
    }

    static public <L,R> Collection<Pair<L,R>> create(Iterable<? extends L> l, Iterable<? extends R> r)
    {
        ArrayList<Pair<L,R>> res = new ArrayList<Pair<L, R>>();
        Iterator<? extends L> lIt = l.iterator();
        Iterator<? extends R> rIt = r.iterator();
        while(lIt.hasNext() && rIt.hasNext())
        {
            L lNext = lIt.next();
            R rNext = rIt.next();
            res.add(new Pair<L, R>(lNext,rNext));
        }
        if(lIt.hasNext() || rIt.hasNext())
        {
            throw new IllegalArgumentException("Size mismatch between input arguments");
        }
        return res;
    }
}