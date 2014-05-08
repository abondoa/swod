package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.repository.RepositoryException;

/**
 * Created by alex on 5/8/14.
 */
public interface OlapDenormalizer
{
    public Iterable<? extends String> generate(Resource dataSet) throws RepositoryException;
}
