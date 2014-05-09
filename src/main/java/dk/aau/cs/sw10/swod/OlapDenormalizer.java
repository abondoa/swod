package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

import java.util.ArrayList;

/**
 * Created by alex on 5/8/14.
 */
public interface OlapDenormalizer
{
    public Iterable<? extends String> generateInstanceDataQueries(Resource dataSet) throws RepositoryException;
    public String getPrefixes() ;
    public Repository generateOntology(Resource dataSet) throws RepositoryException;

}
