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

    protected Iterable<? extends String> generateDimensionQueries(Resource dataSet, String dimension) throws RepositoryException {
        ArrayList<String> levels = new ArrayList<String>(1);
        levels.add(dimension);
        return generateLevelQueries(dataSet,levels,true);
    }

    private Iterable<? extends String> generateLevelQueries(Resource dataSet, ArrayList<String> levels, boolean first) throws RepositoryException {
        ArrayList<String> res = new ArrayList<String>(1);
        String nextLevelQuery = getPrefixes()+"SELECT ?level WHERE " +
                "{ " +
                " <" + levels.get(levels.size() - 1) + "> qb4o:parentLevel ?level . " +
                "}" ;
        ArrayList<String> nextLevels = new ArrayList<String>();
        try {
            for(String level : getParentLevels(dataSet,levels.get(levels.size() - 1)))
            {
                ArrayList<String> levelsTemp = new ArrayList<String>(levels);
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
                "{\n";
        if(first)
        {
            query += "    ?fact <"+levels.get(0)+"> ?level0 .\n";
        }
        query +="    ?level0 ?p_level ?o_level .\n" +
                "}\n" +
                "where\n" +
                "{\n" +
                "    ?fact qb:dataSet <"+dataSet.stringValue()+"> .\n" +
                "    ?fact <"+levels.get(0)+"> ?level0 .\n";

        for(int i = 1 ; i < levels.size() ; ++i)
        {
            query += "    ?level"+(i-1)+" <"+levels.get(i)+"> ?level"+i+".\n";
        }
        query +="    ?level"+ (levels.size()-1)+" ?p_level ?o_level .\n" ;


        query += "    FILTER(   \n";
        for(String dim : nextLevels)
        {
            query += "    ?p_level != <"+dim+"> &&\n";
        }
        query += "    ?p_level != skos:broader\n";
        query += "    ) .\n";

        query += "} ";
        res.add(query);
        return res;
    }
}
