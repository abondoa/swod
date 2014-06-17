package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
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
    protected String regex;
    protected String replace;

    protected RepositoryConnection getInputConnection() {
        return inputConnection;
    }
    public String getPrefixes() {
        return prefixes;
    }
    public OlapDenormalizerAbstract(RepositoryConnection inputConnection) {
        this(inputConnection,"","");
    }

    public OlapDenormalizerAbstract(RepositoryConnection inputConnection, String regex, String replace) {
        this.inputConnection = inputConnection;
        this.prefixes =
                "prefix skos:           <http://www.w3.org/2004/02/skos/core#>\n" +
                        "prefix qb:             <http://purl.org/linked-data/cube#>\n" +
                        "prefix qb4o:           <http://publishing-multidimensional-data.googlecode.com/git/index.html#ref_qbplus_>\n" +
                        "prefix ltpch:           <http://extbi.lab.aau.dk/ontology/ltpch/>\n" +
                        "prefix ltpch-inst:      <http://extbi.lab.aau.dk/resource/ltpch/>\n" +
                        "prefix owl:            <http://www.w3.org/2002/07/owl#>\n" +
                        "prefix rdf:            <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "prefix xml:            <http://www.w3.org/XML/1998/namespace>\n" +
                        "prefix xsd:            <http://www.w3.org/2001/XMLSchema#>\n" +
                        "prefix rdfs:           <http://www.w3.org/2000/01/rdf-schema#>\n";
        this.regex = regex;
        this.replace = replace;
    }

    protected  ArrayList<URI> getDimensions(Resource dataSet) throws  RepositoryException {
        String dimensionQuery = getPrefixes()+
                "SELECT ?level WHERE " +
                "{ " +
                " <" + dataSet.stringValue() + "> qb:structure ?structure . " +
                " ?structure qb:component ?component . " +
                " ?component qb4o:level ?level . " +
                " OPTIONAL { ?component qb:order ?order } . " +
                "} " +
                "order by ASC(?order) " ;

        ArrayList<URI> dimensions = new ArrayList<URI>();

        try {
            TupleQueryResult result = inputConnection.prepareTupleQuery(QueryLanguage.SPARQL, dimensionQuery).evaluate();
            while(result.hasNext())
            {
                BindingSet next = result.next();
                dimensions.add((URI)next.getBinding("level").getValue());
            }
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }
        return dimensions;
    }

    protected  ArrayList<URI> getMeasures(Resource dataSet) throws  RepositoryException {
        String measureQuery = getPrefixes()+
                "SELECT ?measure WHERE " +
                "{ " +
                "    <" + dataSet.stringValue() + "> qb:structure ?structure . " +
                "    ?structure qb:component ?component . " +
                "    ?component qb:measure ?measure . " +
                "}" ;

        ArrayList<URI> measures = new ArrayList<URI>();

        try {
            TupleQueryResult result = inputConnection.prepareTupleQuery(QueryLanguage.SPARQL, measureQuery).evaluate();
            while(result.hasNext())
            {
                BindingSet next = result.next();
                measures.add((URI) next.getBinding("measure").getValue());
            }
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }
        return measures;
    }
    public static String implode(String separator, String... data) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length - 1; i++) {
            //data.length - 1 => to not add separator at the end
            if (!data[i].matches(" *")) {//empty string are ""; " "; "  "; and so on
                sb.append(data[i]);
                sb.append(separator);
            }
        }
        sb.append(data[data.length - 1]);
        return sb.toString();
    }
    public static String implode(String separator,ArrayList<String> data) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.size() - 1; i++) {
            //data.length - 1 => to not add separator at the end
            if (!data.get(i).matches(" *")) {//empty string are ""; " "; "  "; and so on
                sb.append(data.get(i));
                sb.append(separator);
            }
        }
        sb.append(data.get(data.size() - 1));
        return sb.toString();
    }

    protected Iterable<? extends String> generateQueriesForDataSet(Resource dataSet) throws RepositoryException {
        ArrayList<URI> measures = getMeasures(dataSet);
        ArrayList<URI> dimensions = getDimensions(dataSet);
        ArrayList<String> queries = generateObservationQueries(dataSet,dimensions);

        for(URI dimension : dimensions)
        {
            queries.addAll(generateDimensionQueries(dataSet,dimension));
        }

        return queries;
    }

    protected ArrayList<? extends String> generateDimensionQueries(Resource dataSet, URI dimension) throws RepositoryException {
        ArrayList<URI> levels = new ArrayList<URI>(1);
        levels.add(dimension);
        return generateLevelQueries(dataSet,levels,true);
    }

    protected abstract ArrayList<? extends String> generateLevelQueries(Resource dataSet, ArrayList<URI> levels, boolean b) throws RepositoryException;

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
    protected ArrayList<? extends URI> getParentLevels(Resource dataSet, URI level) throws RepositoryException {
        ArrayList<URI> res = new ArrayList<URI>();
        String nextLevelQuery = getPrefixes()+"SELECT ?level WHERE " +
                "{ " +
                " <" + level + "> qb4o:parentLevel ?level . " +
                "}" ;

        TupleQueryResult result = null;
        try {
            result = getInputConnection().prepareTupleQuery(QueryLanguage.SPARQL, nextLevelQuery).evaluate();
            while(result.hasNext())
            {
                BindingSet next = result.next();
                res.add(((URI) next.getBinding("level").getValue()));
            }
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }
        return res;
    }

    protected ArrayList<? extends URI> getOutgoingPropertiesOfLevel(URI levelProperty) throws RepositoryException {
        ArrayList<URI> res = new ArrayList<URI>();
        String nextLevelQuery = getPrefixes()+
                "SELECT ?attribute WHERE " +
                "{ " +
                " <" + levelProperty + "> qb4o:hasAttribute ?attribute . " +
                "}" ;

        TupleQueryResult result = null;
        try {
            result = getInputConnection().prepareTupleQuery(QueryLanguage.SPARQL, nextLevelQuery).evaluate();
            while(result.hasNext())
            {
                BindingSet next = result.next();
                URI attribute = (URI) next.getBinding("attribute").getValue();
                res.add((attribute));
            }
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }
        return res;
    }

    protected boolean isA(URI resource, URI type) throws RepositoryException {
        String typeTripleExistsQuery = getPrefixes()+
                "ASK " +
                "{ " +
                " <" + resource + "> a <"+type+"> . " +
                "}" ;

        try {
            return getInputConnection().prepareBooleanQuery(QueryLanguage.SPARQL, typeTripleExistsQuery).evaluate();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }
        return false;
    }
}
