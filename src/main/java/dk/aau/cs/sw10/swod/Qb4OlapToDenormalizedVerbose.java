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
 * Created by alex on 5/8/14.
 */
public class Qb4OlapToDenormalizedVerbose extends Qb4OlapToStarVerbose
{
    public Qb4OlapToDenormalizedVerbose(RepositoryConnection inputConnection) {
        super(inputConnection);
    }

    public Qb4OlapToDenormalizedVerbose(RepositoryConnection inputConnection, String regex, String replace) {
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

    @Override
    protected ArrayList<? extends String> generateDimensionQueries(Resource dataSet, URI dimension) throws RepositoryException {
        ArrayList<URI> levels = new ArrayList<URI>(1);
        levels.add(dimension);
        return generateLevelQueries(dataSet,levels,true);
    }

    public Repository generateOntology(Resource dataSet) throws RepositoryException
    {
        Repository repo = new SailRepository(new MemoryStore());
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
