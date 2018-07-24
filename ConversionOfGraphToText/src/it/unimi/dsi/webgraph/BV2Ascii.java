package it.unimi.dsi.webgraph;

import java.lang.reflect.InvocationTargetException;
import java.io.*;

/** The main method of this class loads an arbitrary {@link it.unimi.dsi.webgraph.ImmutableGraph}
 * and performs a sequential scan to establish the minimum, maximum and average outdegree.
 */

public class BV2Ascii {

    private BV2Ascii() {}

    static public void main( String arg[] ) throws ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, IOException {
    	

        
        
    	final ImmutableGraph graph = ImmutableGraph.loadOffline( "H:\\Downloads\\uk-2007-05" );

    	NodeIterator nodeIterator = graph.nodeIterator();
    	int curr, d;
    	int[] suc;
    	BufferedOutputStream outStreamB = new BufferedOutputStream(new FileOutputStream("H:\\adjacency_list.docx"), 4096);
    	PrintStream outStream = new PrintStream( outStreamB );

    	while( nodeIterator.hasNext() ) {
    		curr = nodeIterator.nextInt();
    		d = nodeIterator.outdegree();
    		suc = nodeIterator.successorArray();

    		outStream.print( curr );
    		for( int j=0; j<d; j++ ) 
                {
    			outStream.print( " " + (suc[j]) );
    		}
    		outStream.println();
                
    	}
        
        System.out.println ("SUCCESS..!!");
    }
}