package dk.aau.cs.sw10.swod;

import org.apache.commons.cli.*;
import org.openrdf.model.Resource;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.QueryParserRegistry;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.turtle.TurtleParserFactory;
import org.openrdf.sail.memory.MemoryStore;

import java.io.*;

/**
 * Created by alex on 5/6/14.
 */
public class Main
{
    private static Options options = new Options();

    public static void main(String args[]) throws Exception
    {
        options.addOption(OptionBuilder.withLongOpt("ontology")
                .hasArg()
                .withArgName("file")
                .isRequired()
                .withDescription("ontology file used to construct conversion scripts")
                .create());

        RDFParserRegistry.getInstance().add(new TurtleParserFactory());
        QueryParserRegistry.getInstance().add(new SPARQLParserFactory());

        org.openrdf.repository.Repository repo = new SailRepository(new MemoryStore());
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = parser.parse(options,args);

        File inputFile = new File(commandLine.getOptionValue("ontology"));
        con.add(inputFile, "", RDFFormat.TURTLE);//forFileName(inputFile.getName()));

        Resource cube = new Resource() {
            @Override
            public String stringValue() {
                return "http://extbi.lab.aau.dk/ontology/ltpch/lineitemCube";
            }
        };
        int i = 0;
        OlapDenormalizer converter = new Qb4OlapToDenormalizedVerbose(con,".*[/#_]","");
        for ( String q :converter.generateInstanceDataQueries(cube))
        {
            File f = new File("tmp/"+ i++ +".spql");
            Writer w = new FileWriter(f);
            w.write(converter.getPrefixes());
            w.write(q);
            w.flush();;
            w.close();
        }
        RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, new FileWriter("meta.ttl"));
        Repository rep = converter.generateOntology(cube);
        rep.getConnection().prepareGraphQuery(QueryLanguage.SPARQL,
                "CONSTRUCT {?s ?p ?o } WHERE {?s ?p ?o } ").evaluate(writer);
        return;
    }
}
