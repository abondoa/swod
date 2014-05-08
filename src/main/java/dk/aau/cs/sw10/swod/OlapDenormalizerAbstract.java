package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.ArrayList;

/**
 * Created by alex on 5/8/14.
 */
abstract public class OlapDenormalizerAbstract implements OlapDenormalizer {
    private String prefixes;

    private RepositoryConnection inputConnection;

    protected RepositoryConnection getInputConnection() {
        return inputConnection;
    }
    public String getPrefixes() {
        return prefixes;
    }
    public OlapDenormalizerAbstract(RepositoryConnection inputConnection) {
        this.inputConnection = inputConnection;
        this.prefixes =
                "prefix skos:           <http://www.w3.org/2004/02/skos/core#>\n" +
                        "prefix qb:             <http://purl.org/linked-data/cube#>\n" +
                        "prefix qb4o:           <http://publishing-multidimensional-data.googlecode.com/git/index.html#ref_qbplus_>\n" +
                        "prefix rdfh:           <http://lod2.eu/schemas/rdfh#>\n" +
                        "prefix rdfh-inst:      <http://lod2.eu/schemas/rdfh-inst#>\n" +
                        "prefix owl:            <http://www.w3.org/2002/07/owl#>\n" +
                        "prefix rdf:            <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "prefix xml:            <http://www.w3.org/XML/1998/namespace>\n" +
                        "prefix xsd:            <http://www.w3.org/2001/XMLSchema#>\n" +
                        "prefix rdfs:           <http://www.w3.org/2000/01/rdf-schema#>\n";
    }

    protected Iterable<? extends String> generateQueriesForDataSet(Resource dataSet) throws RepositoryException {
        String measureQuery = getPrefixes()+"SELECT ?measure WHERE " +
                "{ " +
                " <" + dataSet.stringValue() + "> qb:structure ?structure . " +
                " ?structure qb:component ?component . " +
                " ?component qb:measure ?measure . " +
                "}" ;

        String dimensionQuery = getPrefixes()+"SELECT ?level WHERE " +
                "{ " +
                " <" + dataSet.stringValue() + "> qb:structure ?structure . " +
                " ?structure qb:component ?component . " +
                " ?component qb4o:level ?level ;" +
                "            qb:order ?order . " +
                "} " +
                "order by ASC(?order) " ;

        ArrayList<String> measures = new ArrayList<String>();
        ArrayList<URI> dimensions = new ArrayList<URI>();
        ArrayList<String> queries = new ArrayList<String>();

        try {
            TupleQueryResult result = inputConnection.prepareTupleQuery(QueryLanguage.SPARQL, measureQuery).evaluate();
            while(result.hasNext())
            {
                BindingSet next = result.next();
                measures.add(((URI) next.getBinding("measure").getValue()).toString());
            }
            result = inputConnection.prepareTupleQuery(QueryLanguage.SPARQL, dimensionQuery).evaluate();
            while(result.hasNext())
            {
                BindingSet next = result.next();
                dimensions.add((URI)next.getBinding("level").getValue());
            }

            queries.addAll(generateObservationQueries(dataSet,dimensions));

            for(URI dimension : dimensions)
            {
                queries.addAll(generateDimensionQueries(dataSet,dimension));
            }
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }

        return queries;
    }

    protected abstract ArrayList<? extends String> generateDimensionQueries(Resource dataSet, URI dimension) throws RepositoryException;

    protected ArrayList<? extends String> generateObservationQueries(Resource dataSet, ArrayList<URI> dimensions) {
        String query = "construct \n" +
                "{\n" +
                "    ?li ?p_li ?o_li .\n" +
                "}\n" +
                "where\n" +
                "{\n" +
                "    ?li qb:dataSet <"+dataSet.stringValue()+"> .\n" +
                "    ?li ?p_li ?o_li .\n";
        if(!dimensions.isEmpty())
        {
            query += " FILTER(   \n";
            for(URI dim : dimensions)
            {
                query += " ?p_li != <"+dim+"> &&\n";
            }
            query = query.substring(0,query.length() - 4);
            query += " ) .\n";
        }
        query += " } ";
        ArrayList<String> res = new ArrayList<String>(1);
        res.add(query);
        return res;
    }
    protected Iterable<? extends URI> getParentLevels(Resource dataSet, URI level) throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        ArrayList<URI> res = new ArrayList<URI>();
        String nextLevelQuery = getPrefixes()+"SELECT ?level WHERE " +
                "{ " +
                " <" + level + "> qb4o:parentLevel ?level . " +
                "}" ;

        TupleQueryResult result = getInputConnection().prepareTupleQuery(QueryLanguage.SPARQL, nextLevelQuery).evaluate();
        while(result.hasNext())
        {
            BindingSet next = result.next();
            res.add(((URI) next.getBinding("level").getValue()));
        }
        return res;
    }
}
