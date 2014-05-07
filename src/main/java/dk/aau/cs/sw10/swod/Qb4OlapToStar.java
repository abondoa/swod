package dk.aau.cs.sw10.swod;

import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.ArrayList;

/**
 * Created by alex on 5/6/14.
 */
public class Qb4OlapToStar
{
    private RepositoryConnection inputConnection;

    public String getPrefixes() {
        return prefixes;
    }

    private String prefixes;

    public Qb4OlapToStar(RepositoryConnection inputConnection) {
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

    /**
     * Generate queries to convert data from QB4OLAP format into Starschema format
     * @param dataSet
     * @return
     */
    public Iterable<? extends String> generate(Resource dataSet) throws RepositoryException {
        this.levels = new ArrayList<String>();
        return  generateQueriesForDataSet(dataSet);
    }

    private Iterable<? extends String> generateQueriesForDataSet(Resource dataSet) throws RepositoryException {
        String measureQuery = prefixes+"SELECT ?measure WHERE " +
                "{ " +
                " <" + dataSet.stringValue() + "> qb:structure ?structure . " +
                " ?structure qb:component ?component . " +
                " ?component qb:measure ?measure . " +
                "}" ;

        String dimensionQuery = prefixes+"SELECT ?level WHERE " +
                "{ " +
                " <" + dataSet.stringValue() + "> qb:structure ?structure . " +
                " ?structure qb:component ?component . " +
                " ?component qb4o:level ?level ;" +
                "            qb:order ?order . " +
                "} " +
                "order by ASC(?order) " ;

        ArrayList<String> measures = new ArrayList<String>();
        ArrayList<String> dimensions = new ArrayList<String>();
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
                dimensions.add(next.getBinding("level").getValue().stringValue());
            }

            for(String query : generateObservationQueries(dataSet,dimensions))
            {
                queries.add(query);
            }

            for(String dimension : dimensions)
            {
                for(String query : generateDimensionQueries(dataSet,dimension))
                {
                    queries.add(query);
                }
            }
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }

        return queries;
    }

    private Iterable<? extends String> generateObservationQueries(Resource dataSet, ArrayList<String> dimensions) {
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
            for(String dim : dimensions)
            {
                query += " ?p_li != <"+dim+"> &&\n";
            }
            query = query.substring(0,query.length() - 4);
            query += " ) .";
        }
        query += " } ";
        ArrayList<String> res = new ArrayList<String>(1);
        res.add(query);
        return res;
    }

    private Iterable<? extends String> generateDimensionQueries(Resource dataSet, String dimension) throws RepositoryException {
        ArrayList<String> levels = new ArrayList<String>(1);
        levels.add(dimension);
        return generateLevelQueries(dataSet,levels,true);
    }

    private ArrayList<String> levels;

    private Iterable<? extends String> generateLevelQueries(Resource dataSet, ArrayList<String> levels, boolean first) throws RepositoryException {
        if(this.levels.contains(levels.get(levels.size()-1))){
            //return new ArrayList<String>();
        }
        ArrayList<String> res = new ArrayList<String>(1);
        this.levels.add(levels.get(levels.size()-1));
        String nextLevelQuery = prefixes+"SELECT ?level WHERE " +
                "{ " +
                " <" + levels.get(levels.size() - 1) + "> qb4o:parentLevel ?level . " +
                "}" ;
        ArrayList<String> nextLevels = new ArrayList<String>();
        try {
            TupleQueryResult result = inputConnection.prepareTupleQuery(QueryLanguage.SPARQL, nextLevelQuery).evaluate();
            while(result.hasNext())
            {
                ArrayList<String> levelsTemp = new ArrayList<String>(levels);
                BindingSet next = result.next();
                levelsTemp.add(((URI) next.getBinding("level").getValue()).toString());
                for(String levelQuery : generateLevelQueries(dataSet, levelsTemp, false))
                {
                    res.add(levelQuery);
                }
                nextLevels.add(((URI) next.getBinding("level").getValue()).toString());
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

        if(!nextLevels.isEmpty())
        {
            query += " FILTER(   \n";
            for(String dim : nextLevels)
            {
                query += " ?p_level != <"+dim+"> &&\n";
            }
            query = query.substring(0,query.length() - 4);
            query += "\n    ) .\n";
        }
        query += "} ";
        res.add(query);
        return res;
    }
}
