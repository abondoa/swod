package dk.aau.cs.sw10.swod;

import org.apache.commons.cli.*;
import org.openrdf.model.Resource;
import org.openrdf.query.parser.QueryParserRegistry;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
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

        int i = 0;
        OlapDenormalizer converter = new Qb4OlapToDenormalized(con);
        for ( String q :converter.generateInstanceDataQueries(new Resource() {
            @Override
            public String stringValue() {
                return "http://lod2.eu/schemas/rdfh#lineitemCube";
            }
        }))
        {
            File f = new File("tmp/"+ i++ +".spql");
            Writer w = new FileWriter(f);
            w.write(converter.getPrefixes());
            w.write(q);
            w.flush();;
            w.close();
        }
    }
}
