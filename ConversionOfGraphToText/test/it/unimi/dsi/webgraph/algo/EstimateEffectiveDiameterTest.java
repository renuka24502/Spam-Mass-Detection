package it.unimi.dsi.webgraph.algo;

/*		 
 * Copyright (C) 2010-2014 Paolo Boldi & Sebastiano Vigna 
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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.CliqueGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.WebGraphTestCase;

import java.io.IOException;

import org.junit.Test;


public class EstimateEffectiveDiameterTest extends WebGraphTestCase {

	@Test
	public void testSmall() throws IOException {
		final ImmutableGraph g = ArrayListMutableGraph.newBidirectionalCycle( 40 ).immutableView();

		final HyperBall hyperBall = new HyperBall( g, 8, 0 );
		hyperBall.run( Integer.MAX_VALUE, -1 );
 		assertEquals( 17, NeighbourhoodFunction.effectiveDiameter( .9, hyperBall.neighbourhoodFunction.toDoubleArray() ), 1 );
 		hyperBall.close();
	}
	
	@Test
	public void testCycleOfCliques() throws IOException {
		ArrayListMutableGraph mg = new ArrayListMutableGraph();
		// Creates a bidirectional cycle of k n-cliques, each connected with the next by 2*b<n arcs
		// Expected diameter: k + 1
		final int n = 20, k = 100, b = 6;
		mg.addNodes( n * k );
		for ( int i = 0; i < k; i++ ) 
			for ( int j = 0; j < n; j++ )
				for ( int h = 0; h < n; h++ )
					mg.addArc( n * i + j, n * i + h );
		for ( int i = 0; i < k; i++ )
			for ( int j = 0; j < b; j++ ) {
				mg.addArc( n * i + j, n * ( ( i + 1 ) % k ) + n - 1 - j );
				mg.addArc( n * ( ( i + 1 ) % k ) + n - 1 - j, n * i + j );
			}
		ImmutableGraph g = mg.immutableView();

		final HyperBall hyperBall = new HyperBall( g, 8, 0 );
		hyperBall.run( Integer.MAX_VALUE, -1 );
		double estimation = NeighbourhoodFunction.effectiveDiameter( 1, hyperBall.neighbourhoodFunction.toDoubleArray() );
		double expected = k + 1;
		double relativeError = Math.abs( estimation - expected ) / expected;
		System.err.println( "Estimate: " + estimation );
		System.err.println( "Relative error in estimate (should be <0.05): " + relativeError );
		assertTrue( relativeError < 0.05 ); // Accept error within 5%

		hyperBall.close();
	}

	@Test
	public void testTwoCyclesOfCliques() throws IOException {
		ArrayListMutableGraph mg = new ArrayListMutableGraph();
		// Creates two bidirectional cycles of k n-cliques (kx nx-cliques, resp.), each connected with the next by 2*b<n (2*bx<nx, resp.) arcs
		// We expect that more than 90% of the pairs are within distance k or kx, so the effective diameter should be k
		final int n = 16, k = 10, b = 6;
		final int firstNodeSecondClique = n * k;
		final int nx = 3, kx = 5, bx = 1;
		mg.addNodes( n * k + nx * kx );
		for ( int i = 0; i < k; i++ ) 
			for ( int j = 0; j < n; j++ )
				for ( int h = 0; h < n; h++ )
					mg.addArc( n * i + j, n * i + h );
		for ( int i = 0; i < k; i++ )
			for ( int j = 0; j < b; j++ ) {
				mg.addArc( n * i + j, n * ( ( i + 1 ) % k ) + n - 1 - j );
				mg.addArc( n * ( ( i + 1 ) % k ) + n - 1 - j, n * i + j );
			}
		for ( int i = 0; i < kx; i++ ) 
			for ( int j = 0; j < nx; j++ )
				for ( int h = 0; h < nx; h++ )
					mg.addArc( firstNodeSecondClique + nx * i + j, firstNodeSecondClique + nx * i + h );
		for ( int i = 0; i < kx; i++ )
			for ( int j = 0; j < bx; j++ ) {
				mg.addArc( firstNodeSecondClique + nx * i + j, firstNodeSecondClique + nx * ( ( i + 1 ) % kx ) + nx - 1 - j );
				mg.addArc( firstNodeSecondClique + nx * ( ( i + 1 ) % kx ) + nx - 1 - j, firstNodeSecondClique + nx * i + j );
			}
		final HyperBall hyperBall = new HyperBall( mg.immutableView(), 8, 0 );
		hyperBall.run( Integer.MAX_VALUE, -1 );
		
		assertEquals( k, NeighbourhoodFunction.effectiveDiameter( .99, hyperBall.neighbourhoodFunction.toDoubleArray() ), 1 );
		
		hyperBall.close();
	}
	
	
	@Test
	public void testCliqueGraph() throws IOException {
		HyperBall hyperBall = new HyperBall( new CliqueGraph( 100, 5 ), 8, 0 );
		hyperBall.run( 1000, 1E-3 );
		hyperBall.close();
	}
	
	@Test
	public void testLarge() throws IOException {
		String path = getGraphPath( "cnr-2000" );
		ImmutableGraph g = ImmutableGraph.load( path ); 
		final HyperBall hyperBall = new HyperBall( g, 8, 0 );
		hyperBall.run( Integer.MAX_VALUE, -1 );
		assertEquals( NeighbourhoodFunction.effectiveDiameter( .9, HyperBallSlowTest.cnr2000NF ), NeighbourhoodFunction.effectiveDiameter( .9, hyperBall.neighbourhoodFunction.toDoubleArray() ), 1 );
		hyperBall.close();
	}
}
