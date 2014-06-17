package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import java.util.ArrayList;
import java.util.Stack;

/**
 * Created by alex on 5/6/14.
 */
public class Qb4OlapToStarVerbose extends Qb4OlapToStar
{

    private ArrayList<String> levels;

    public Qb4OlapToStarVerbose(RepositoryConnection inputConnection) {
        super(inputConnection);
    }

    public Qb4OlapToStarVerbose(RepositoryConnection inputConnection, String regex, String replace) {
        super(inputConnection,regex,replace);
    }

    @Override
    protected Iterable<? extends String> generateQueriesForDataSet(Resource dataSet) throws RepositoryException {
        ArrayList<URI> measures = getMeasures(dataSet);
        ArrayList<URI> dimensions = getDimensions(dataSet);
        ArrayList<String> queries = generateObservationQueries(dataSet,dimensions);

        String query = "construct \n" +
                "{\n" +
                "    ?dim ?star_predicate ?o_level ;\n" +
                "         qb4o:inLevel ?inLevel .\n" +
                "}\n" +
                "where\n" +
                "{ {";
        ArrayList<String> dimQueries = new ArrayList<String>();
        for(URI dimension : dimensions)
        {
            dimQueries.addAll(generateDimensionQueries(dataSet,dimension));
        }
        query += implode("\n}UNION{\n",dimQueries) + "\n}\n }";
        queries.add(query);
        return queries;
    }

    protected ArrayList<? extends String> generateDimensionQueries(Resource dataSet, URI dimension) throws RepositoryException {
        ArrayList<URI> levels = new ArrayList<URI>(1);
        levels.add(dimension);
        return generateLevelQueries(dataSet,levels,false);
    }

    protected ArrayList<? extends String> generateLevelQueries(Resource dataSet, ArrayList<URI> levels, boolean first) throws RepositoryException {
        ArrayList<String> res = new ArrayList<String>(1);
        ArrayList<URI> nextLevels = new ArrayList<URI>();
        URI currentLevel = levels.get(levels.size() - 1);
        URI dimension = levels.get(0);
        int correctForFact = first ? 0 : 1;
        for(URI level : getParentLevels(dataSet,currentLevel))
        {
            ArrayList<URI> levelsTemp = new ArrayList<URI>(levels);
            levelsTemp.add(level);
            for(String levelQuery : generateLevelQueries(dataSet, levelsTemp, first))
            {
                res.add(levelQuery);
            }
            nextLevels.add(level);
        }

        String query = "select\n" +
                "    ?dim ?star_predicate ?o_level ?inLevel\n" +
                "where\n" +
                "{\n" +
                "    ?level"+levels.size()+" qb4o:inLevel <"+currentLevel+"> ;\n" +
                "                    ?p_level ?o_level .\n" +
                "    FILTER(   \n";

        for(URI level : nextLevels)
        {
            query +=
                "        ?p_level != <"+level+"> &&\n";
        }
        query +=
                "        ?p_level != rdf:type && \n" +
                "        ?p_level != qb:dataSet && \n" +
                "        ?p_level != skos:broader &&\n" +
                "        ?p_level != qb4o:inLevel\n" +
                "    ) .\n" +
                "    BIND(<"+currentLevel+"> as ?inLevel) .\n" +
                "    BIND(URI(CONCAT(\""+
                currentLevel.getNamespace()+
                        (first ? dimension.getLocalName().replaceAll(regex,replace) + "_" : "") +
                currentLevel.getLocalName().replaceAll(regex,replace)+
                "_\",REPLACE(STR(?p_level),\""+regex+"\",\""+replace+"\"))) as ?star_predicate) .\n";

        for(int i = levels.size()-1 ; i >= correctForFact ; --i)
        {
            query +=
                    "    OPTIONAL {\n" +
                    "       ?level"+i+" <"+levels.get(i)+"> ?level"+(i+1)+".\n";
        }

        for(int i = levels.size()-1 ; i >= correctForFact ; --i)
        {
            query +="    }\n";
        }
        for(int i = levels.size() ; i >= correctForFact ; --i)
        {
            query +=
                    "    OPTIONAL { BIND(?level"+i+" as ?dim) . }\n";
        }
        query += "}\n";
        res.add(query);
        return res;
    }

    @Override
    protected ArrayList<String> generateObservationQueries(Resource dataSet, ArrayList<URI> dimensions) {
    String query =
            "construct \n" +
                    "{\n" +
                    "    ?li ?p_li ?o_li .\n" +
                    "}\n" +
                    "where\n" +
                    "{\n" +
                    "    ?li qb:dataSet <"+dataSet.stringValue()+"> .\n" +
                    "    ?li ?p_li ?o_li .\n";
    query += " } ";
    ArrayList<String> res = new ArrayList<String>(1);
    res.add(query);
    return res;
}

    public Repository generateOntology(Resource dataSet) throws RepositoryException
    {
        Repository repo = new SailRepository(new MemoryStore());
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
                else
                {
                    con.add(
                            con.getValueFactory().createStatement(
                                    con.getValueFactory().createURI(
                                            level.getNamespace()+
                                                    level.getLocalName().replaceAll(regex,replace) + "_" +
                                                    property.getLocalName().replaceAll(regex,replace)),
                                    con.getValueFactory().createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                                    con.getValueFactory().createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")
                            )
                    );
                    con.add(
                            con.getValueFactory().createStatement(
                                    con.getValueFactory().createURI(
                                            level.getNamespace()+
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
