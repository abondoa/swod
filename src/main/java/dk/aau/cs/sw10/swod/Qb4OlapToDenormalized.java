package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
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

    public Qb4OlapToDenormalized(RepositoryConnection inputConnection,String regex,String replace) {
        super(inputConnection,regex,replace);
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
                "    BIND(URI(CONCAT(\""+
                dimension.getNamespace()+
                dimension.getLocalName().replaceAll(regex,replace)+"_"+
                currentLevel.getLocalName().replaceAll(regex,replace)+
                "_\",REPLACE(REPLACE(STR(?p),\"^.*[/#]\",\"\"),\""+regex+"\",\""+replace+"\"))) as ?denorm_predicate) .\n";


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
                if(! isA(property, con.getValueFactory().createURI("http://www.w3.org/2002/07/owl#InverseFunctionalProperty")))
                {
                    con.add(
                            con.getValueFactory().createStatement(
                                    con.getValueFactory().createURI(
                                            dim.getNamespace()+
                                            dim.getLocalName().replaceAll(regex,replace) + "_" +
                                            level.getLocalName().replaceAll(regex,replace) + "_" +
                                            property.getLocalName().replaceAll(regex,replace)),
                                    con.getValueFactory().createURI("http://www.w3.org/2000/01/rdf-schema#subPropertyOf"),
                                    property
                            )
                    );
                }
                else
                {
                    con.add(
                            con.getValueFactory().createStatement(
                                    con.getValueFactory().createURI(
                                            dim.getNamespace()+
                                                    dim.getLocalName().replaceAll(regex,replace) + "_" +
                                                    level.getLocalName().replaceAll(regex,replace) + "_" +
                                                    property.getLocalName().replaceAll(regex,replace)),
                                    con.getValueFactory().createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                                    con.getValueFactory().createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")
                            )
                    );
                    con.add(
                            con.getValueFactory().createStatement(
                                    con.getValueFactory().createURI(
                                            dim.getNamespace()+
                                                    dim.getLocalName().replaceAll(regex,replace) + "_" +
                                                    level.getLocalName().replaceAll(regex,replace) + "_" +
                                                    property.getLocalName().replaceAll(regex,replace)),
                                    con.getValueFactory().createURI("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
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
