package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.ArrayList;

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
    public Iterable<? extends String> generate(Resource dataSet) throws RepositoryException {
        return  generateQueriesForDataSet(dataSet);
    }

    protected ArrayList<? extends String> generateDimensionQueries(Resource dataSet, URI dimension) throws RepositoryException {
        ArrayList<URI> levels = new ArrayList<URI>(1);
        levels.add(dimension);
        return generateLevelQueries(dataSet,levels,true);
    }

    private ArrayList<? extends String> generateLevelQueries(Resource dataSet, ArrayList<URI> levels, boolean first) throws RepositoryException {
        ArrayList<String> res = new ArrayList<String>(1);
        URI currentLevel = levels.get(levels.size() - 1);
        URI dimension = levels.get(0);
        ArrayList<URI> nextLevels = new ArrayList<URI>();
        try {
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
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }

        String query = "construct \n" +
                "{\n" +
                "    ?fact ?denorm_pred ?o .\n" +
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
                "    BIND(URI(CONCAT(\"http://lod2.eu/schemas/rdfh#\",\""+dimension+"_"+currentLevel+"_\",SUBSTR(STR(?p),29))) as ?denorm_predicate) .\n";


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
}
