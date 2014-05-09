package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Stack;

/**
 * Created by alex on 5/8/14.
 */
public class Qb4OlapToDenormalized extends OlapDenormalizerAbstract
{
    public Qb4OlapToDenormalized(RepositoryConnection inputConnection) {
        super(inputConnection);
    }

    /**
     * Generate queries to convert data from QB4OLAP format into Denormalizedschema format
     * @param dataSet
     * @return
     */
    public Iterable<? extends String> generateInstanceDataQueries(Resource dataSet) throws RepositoryException {
        return  generateQueriesForDataSet(dataSet);
    }

    protected ArrayList<? extends String> generateLevelQueries(Resource dataSet, ArrayList<URI> levels, boolean first) throws RepositoryException {
        ArrayList<String> res = new ArrayList<String>(1);
        URI currentLevel = levels.get(levels.size() - 1);
        URI dimension = levels.get(0);
        ArrayList<URI> nextLevels = new ArrayList<URI>();

        for(URI level : getParentLevels(dataSet,currentLevel))
        {
            ArrayList<URI> levelsTemp = new ArrayList<URI>(levels);
            levelsTemp.add(level);
            for(String levelQuery : generateLevelQueries(dataSet, levelsTemp, false))
            {
                res.add(levelQuery);
            }
            nextLevels.add(level);
        }

        String query =
                "construct \n" +
                "{\n" +
                "    ?fact ?denorm_predicate ?o .\n" +
                "}\n" +
                "where\n" +
                "{\n" +
                "    ?fact qb:dataSet <"+dataSet.stringValue()+"> .\n" +
                "    ?fact <"+levels.get(0)+"> ?level0 .\n";

        for(int i = 1 ; i < levels.size() ; ++i)
        {
            query += "    ?level"+(i-1)+" <"+levels.get(i)+"> ?level"+i+".\n";
        }
        query +="    ?level"+ (levels.size()-1)+" ?p ?o .\n" +
                "    BIND(URI(CONCAT(\""+dimension+"_"+currentLevel.getLocalName()+"_\",REPLACE(STR(?p),\"^.*[/#]\",\"\"))) as ?denorm_predicate) .\n";


        query += "    FILTER(   \n";
        for(URI dim : nextLevels)
        {
            query += "    ?p != <"+dim+"> &&\n";
        }
        query +="    ?p != rdf:type && \n" +
                "    ?p != qb:dataSet && \n" +
                "    ?p != skos:broader &&\n" +
                "    ?p != qb4o:inLevel\n"+
                "    ) .\n";

        query += "} ";
        res.add(query);
        return res;
    }

    public Repository generateOntology(Resource dataSet) throws RepositoryException
    {
        org.openrdf.repository.Repository repo = new SailRepository(new MemoryStore());
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        Stack<Pair<URI,URI>> levelsToProcess = new Stack<Pair<URI,URI>>();
        ArrayList<URI> dimensions = getDimensions(dataSet);
        levelsToProcess.addAll(Pair.create(dimensions, dimensions));
        while(!levelsToProcess.empty())
        {
            Pair<URI,URI> dimLevel = levelsToProcess.pop();
            URI level = dimLevel.getRight();
            URI dim = dimLevel.getLeft();
            for(URI parentLevel :getParentLevels(dataSet,level))
            {
                levelsToProcess.add(new Pair<URI, URI>(dim,parentLevel));
            }
            //Do processing
            for(URI property : getOutgoingPropertiesOfLevel(level))
            {

            }
        }
        con.close();
        return repo;
    }

    private Iterable<? extends URI> getOutgoingPropertiesOfLevel(URI level) {

        return null;
    }
}
