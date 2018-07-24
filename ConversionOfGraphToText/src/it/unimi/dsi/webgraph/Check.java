package it.unimi.dsi.webgraph;

/*		 
 * Copyright (C) 2011-2014 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import static it.unimi.dsi.webgraph.Transform.ensureNumArgs;
import static it.unimi.dsi.webgraph.Transform.load;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** Static methods that check properties of immutable graphs. */

public class Check {

	private static final Logger LOGGER = LoggerFactory.getLogger( Check.class );
	
	private Check() {}

	/** Check whether a graph is symmetric using {@link Transform#transpose(ImmutableGraph, ProgressLogger)}.
	 * 
	 * @param graph a graph.
	 * @return whether <code>graph</code> is symmetric.
	 */	
	public static boolean symmetry( final ImmutableGraph graph ) {
		return symmetry( graph, null  );
	}

	/** Check whether a graph is symmetric using {@link Transform#transpose(ImmutableGraph, ProgressLogger)}.
	 * 
	 * @param graph a graph.
	 * @param pl passed to {@link Transform#transpose(ImmutableGraph, ProgressLogger)}.
	 * @return whether <code>graph</code> is symmetric.
	 */	
	public static boolean symmetry( final ImmutableGraph graph, final ProgressLogger pl ) {
		return graph.equals( Transform.transpose( graph, pl ) );
	}

	/** Check whether a graph is symmetric using {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * 
	 * @param graph a graph.
	 * @param batchSize passed to {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * @return whether <code>graph</code> is symmetric.
	 */
	public static boolean symmetryOffline(  final ImmutableGraph graph, final int batchSize ) throws IOException {
		return symmetryOffline( graph, batchSize, null );
	}

	/** Check whether a graph is symmetric using {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * 
	 * @param graph a graph.
	 * @param batchSize passed to {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * @param tempDir passed to {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * @return whether <code>graph</code> is symmetric.
	 */
	public static boolean symmetryOffline(  final ImmutableGraph graph, final int batchSize, final File tempDir ) throws IOException {
		return symmetryOffline( graph, batchSize, tempDir, null );
	}

	/** Check whether a graph is symmetric using {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * 
	 * @param graph a graph.
	 * @param batchSize passed to {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * @param tempDir passed to {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * @param pl passed to {@link Transform#transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 * @return whether <code>graph</code> is symmetric.
	 */
	public static boolean symmetryOffline(  final ImmutableGraph graph, final int batchSize, final File tempDir, final ProgressLogger pl ) throws IOException {
		return graph.equals( Transform.transposeOffline( graph, batchSize, tempDir, pl ) );
	}


	public static void main( String args[] ) throws IOException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException {
		Class<?> graphClass = null;
		boolean offline = false, sequential = false;

		SimpleJSAP jsap = new SimpleJSAP( Check.class.getName(), 
				"Checks properties of a graph. All checks require, after the name,\n" +
				"some parameters specified below:\n" +
				"\n" +
				"symmetry             sourceBasename\n" +
				"symmetryOffline      sourceBasename [batchSize] [tempDir]\n" +
				"\n" +
				"Please consult the Javadoc documentation for more information on each check.",
				new Parameter[] {
						new FlaggedOption( "graphClass", GraphClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class to load the graph." ),
						new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
						new Switch( "offline", 'o', "offline", "Use the offline load method to reduce memory consumption." ),
						new Switch( "sequential", 'S', "sequential", "Use the sequential load method to reduce memory consumption." ),
						new UnflaggedOption( "check", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The check." ),
						new UnflaggedOption( "param", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The remaining parameters." ),
					}	
				);
		
		JSAPResult jsapResult = jsap.parse( args );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		graphClass = jsapResult.getClass( "graphClass");
		offline = jsapResult.getBoolean( "offline" );
		sequential = jsapResult.getBoolean( "sequential" );
		String check = jsapResult.getString( "check" );
		String[] param = jsapResult.getStringArray( "param" );
		
		String source[] = null;
		int batchSize = 1000000;
		File tempDir = null;
		
		if ( ! ensureNumArgs( param, -1 ) ) return;
		
		if ( check.equals( "symmetry" ) ) {
			if ( ! ensureNumArgs( param, 1 ) ) return;
			source = new String[] { param[ 0 ] };
		}
		else if ( check.equals( "symmetryOffline" ) ) {
			source = new String[] { param[ 0 ] };
			if ( param.length >= 2 ) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse( param[ 1 ] )).intValue();
				if ( param.length == 3 ) tempDir = new File( param[ 2 ] );
				else if ( ! ensureNumArgs( param, 2 ) )	return;
			}
			else if ( ! ensureNumArgs( param, 1 ) )	return;
		}
		else {
			System.err.println( "Unknown check: " + check );
			return;
		}

		final ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ), TimeUnit.MILLISECONDS );
		final ImmutableGraph[] graph = new ImmutableGraph[ source.length ];
		
		for ( int i = 0; i < source.length; i++ ) 
			if ( source[ i ] == null ) graph[ i ] = null;
			else graph[ i ] = load( graphClass, source[ i ], sequential, offline, pl );
		
		if ( check.equals( "symmetry" ) ) {
			System.out.println( symmetry( graph[ 0 ], pl ) );
		}
		else if ( check.equals( "symmetryOffline" ) ) {
			System.out.println( symmetryOffline( graph[ 0 ], batchSize, tempDir, pl ) );
		}
	}
}
