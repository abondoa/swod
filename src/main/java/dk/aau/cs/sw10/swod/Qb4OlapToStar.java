package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Stack;

/**
 * Created by alex on 5/6/14.
 */
public class Qb4OlapToStar extends OlapDenormalizerAbstract
{

    private ArrayList<String> levels;

    public Qb4OlapToStar(RepositoryConnection inputConnection) {
        super(inputConnection);
    }

    public Qb4OlapToStar(RepositoryConnection inputConnection,String regex,String replace) {
        super(inputConnection,regex,replace);
    }

    /**
     * Generate queries to convert data from QB4OLAP format into Starschema format
     * @param dataSet
     * @return
     */
    public Iterable<? extends String> generateInstanceDataQueries(Resource dataSet) throws RepositoryException {
        this.levels = new ArrayList<String>();
        return  generateQueriesForDataSet(dataSet);
    }

    protected ArrayList<? extends String> generateLevelQueries(Resource dataSet, ArrayList<URI> levels, boolean first) throws RepositoryException {
        ArrayList<String> res = new ArrayList<String>(1);
        ArrayList<URI> nextLevels = new ArrayList<URI>();
        URI currentLevel = levels.get(levels.size() - 1);
        URI dimension = levels.get(0);
        for(URI level : getParentLevels(dataSet,levels.get(levels.size() - 1)))
        {
            ArrayList<URI> levelsTemp = new ArrayList<URI>(levels);
            levelsTemp.add(level);
            for(String levelQuery : generateLevelQueries(dataSet, levelsTemp, false))
            {
                res.add(levelQuery);
            }
            nextLevels.add(level);
        }

        String query = "construct \n" +
                "{\n";
        if(first)
        {
            query += "    ?fact <"+levels.get(0)+"> ?level0 .\n";
        }
        query +="    ?level0 ?star_predicate ?o_level .\n" +
                "}\n" +
                "where\n" +
                "{\n" +
                "    ?fact qb:dataSet <"+dataSet.stringValue()+"> .\n" +
                "    ?fact <"+dimension+"> ?level0 .\n";

        for(int i = 1 ; i < levels.size() ; ++i)
        {
            query += "    ?level"+(i-1)+" <"+levels.get(i)+"> ?level"+i+".\n";
        }
        query +="    ?level"+ (levels.size()-1)+" ?p_level ?o_level .\n" +
                "    BIND(URI(CONCAT(\""+
                currentLevel.getNamespace()+
                currentLevel.getLocalName().replaceAll(regex,replace)+
                "_\",REPLACE(REPLACE(STR(?p_level),\"^.*[/#]\",\"\"),\""+regex+"\",\""+replace+"\"))) as ?star_predicate) .\n";


        query += "    FILTER(   \n";
        for(URI dim : nextLevels)
        {
            query += "    ?p_level != <"+dim+"> &&\n";
        }
        query +="    ?p_level != rdf:type && \n" +
                "    ?p_level != qb:dataSet && \n" +
                "    ?p_level != skos:broader &&\n" +
                "    ?p_level != qb4o:inLevel\n"+
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
        Stack<URI> levelsToProcess = new Stack<URI>();
        ArrayList<URI> dimensions = getDimensions(dataSet);
        levelsToProcess.addAll(dimensions);
        while(!levelsToProcess.empty())
        {
            URI level = levelsToProcess.pop();
            for(URI parentLevel :getParentLevels(dataSet,level))
            {
                levelsToProcess.add(parentLevel);
            }
            //Do processing
            for(URI property : getOutgoingPropertiesOfLevel(level))
            {
                if(! isA(property, con.getValueFactory().createURI("http://www.w3.org/2002/07/owl#InverseFunctionalProperty")))
                {
                    con.add(
                            con.getValueFactory().createStatement(
                                    con.getValueFactory().createURI(
                                            level.getNamespace()+
                                            level.getLocalName().replaceAll(regex,replace) + "_" +
                                            property.getLocalName().replaceAll(regex,replace)),
                                    con.getValueFactory().createURI("http://www.w3.org/2000/01/rdf-schema#subPropertyOf"),
                                    property
                            )
                    );
                }
            }
        }
        con.close();
        return repo;
    }
}
