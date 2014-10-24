/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tez.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.tez.client.LocalTezExecutionEnvironment;

/**
 * This program implements a modified version of the TPC-H query 10.
 * The original query can be found at
 * <a href="http://www.tpc.org/tpch/spec/tpch2.16.0.pdf">http://www.tpc.org/tpch/spec/tpch2.16.0.pdf</a> (page 45).
 * 
 * <p>
 * This program implements the following SQL equivalent:
 * 
 * <p>
 * <code><pre>
 * SELECT 
 *        c_custkey,
 *        c_name, 
 *        c_address,
 *        n_name, 
 *        c_acctbal
 *        SUM(l_extendedprice * (1 - l_discount)) AS revenue,  
 * FROM   
 *        customer, 
 *        orders, 
 *        lineitem, 
 *        nation 
 * WHERE 
 *        c_custkey = o_custkey 
 *        AND l_orderkey = o_orderkey 
 *        AND YEAR(o_orderdate) > '1990' 
 *        AND l_returnflag = 'R' 
 *        AND c_nationkey = n_nationkey 
 * GROUP BY 
 *        c_custkey, 
 *        c_name, 
 *        c_acctbal, 
 *        n_name, 
 *        c_address
 * </pre></code>
 *        
 * <p>
 * Compared to the original TPC-H query this version does not print 
 * c_phone and c_comment, only filters by years greater than 1990 instead of
 * a period of 3 months, and does not sort the result by revenue.
 * 
 * <p>
 * Input files are plain text CSV files using the pipe character ('|') as field separator 
 * as generated by the TPC-H data generator which is available at <a href="http://www.tpc.org/tpch/">http://www.tpc.org/tpch/</a>.
 * 
 * <p>
 * Usage: <code>TPCHQuery10 &lt;customer-csv path&gt; &lt;orders-csv path&gt; &lt;lineitem-csv path&gt; &lt;nation-csv path&gt; &lt;result path&gt;</code><br>
 *  
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> tuple data types
 * <li> inline-defined functions
 * <li> projection and join projection
 * <li> build-in aggregation functions
 * </ul>
 */
@SuppressWarnings("serial")
public class TPCHQuery10 {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		
		if(!parseParameters(args)) {
			return;
		}
		
		final ExecutionEnvironment env = LocalTezExecutionEnvironment.create();

		// get customer data set: (custkey, name, address, nationkey, acctbal) 
		DataSet<Tuple5<Integer, String, String, Integer, Double>> customers = getCustomerDataSet(env);
		// get orders data set: (orderkey, custkey, orderdate)
		DataSet<Tuple3<Integer, Integer, String>> orders = getOrdersDataSet(env);
		// get lineitem data set: (orderkey, extendedprice, discount, returnflag)
		DataSet<Tuple4<Integer, Double, Double, String>> lineitems = getLineitemDataSet(env);
		// get nation data set: (nationkey, name)
		DataSet<Tuple2<Integer, String>> nations = getNationsDataSet(env);

		// orders filtered by year: (orderkey, custkey)
		DataSet<Tuple2<Integer, Integer>> ordersFilteredByYear =
				// filter by year
				orders.filter(
								new FilterFunction<Tuple3<Integer,Integer, String>>() {
									@Override
									public boolean filter(Tuple3<Integer, Integer, String> o) {
										return Integer.parseInt(o.f2.substring(0, 4)) > 1990;
									}
								})
				// project fields out that are no longer required
				.project(0,1).types(Integer.class, Integer.class);

		// lineitems filtered by flag: (orderkey, revenue)
		DataSet<Tuple2<Integer, Double>> lineitemsFilteredByFlag = 
				// filter by flag
				lineitems.filter(new FilterFunction<Tuple4<Integer, Double, Double, String>>() {
										@Override
										public boolean filter(Tuple4<Integer, Double, Double, String> l) {
											return l.f3.equals("R");
										}
								})
				// compute revenue and project out return flag
				.map(new MapFunction<Tuple4<Integer, Double, Double, String>, Tuple2<Integer, Double>>() {
							@Override
							public Tuple2<Integer, Double> map(Tuple4<Integer, Double, Double, String> l) {
								// revenue per item = l_extendedprice * (1 - l_discount)
								return new Tuple2<Integer, Double>(l.f0, l.f1 * (1 - l.f2));
							}
					});

		// join orders with lineitems: (custkey, revenue)
		DataSet<Tuple2<Integer, Double>> revenueByCustomer = 
				ordersFilteredByYear.joinWithHuge(lineitemsFilteredByFlag)
									.where(0).equalTo(0)
									.projectFirst(1).projectSecond(1)
									.types(Integer.class, Double.class)
									.groupBy(0).aggregate(Aggregations.SUM, 1);

		// join customer with nation (custkey, name, address, nationname, acctbal)
		DataSet<Tuple5<Integer, String, String, String, Double>> customerWithNation = customers
						.joinWithTiny(nations)
						.where(3).equalTo(0)
						.projectFirst(0,1,2).projectSecond(1).projectFirst(4)
						.types(Integer.class, String.class, String.class, String.class, Double.class);

		// join customer (with nation) with revenue (custkey, name, address, nationname, acctbal, revenue)
		DataSet<Tuple6<Integer, String, String, String, Double, Double>> result = 
				customerWithNation.join(revenueByCustomer)
				.where(0).equalTo(0)
				.projectFirst(0,1,2,3,4).projectSecond(1)
				.types(Integer.class, String.class, String.class, String.class, Double.class, Double.class);

		// emit result
		result.writeAsCsv(outputPath, "\n", "|");
		
		// execute program
		env.execute("TPCH Query 10 Example");
		
	}
	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static String customerPath;
	private static String ordersPath;
	private static String lineitemPath;
	private static String nationPath;
	private static String outputPath;
	
	private static boolean parseParameters(String[] programArguments) {
		
		if(programArguments.length > 0) {
			if(programArguments.length == 5) {
				customerPath = programArguments[0];
				ordersPath = programArguments[1];
				lineitemPath = programArguments[2];
				nationPath = programArguments[3];
				outputPath = programArguments[4];
			} else {
				System.err.println("Usage: TPCHQuery10 <customer-csv path> <orders-csv path> <lineitem-csv path> <nation-csv path> <result path>");
				return false;
			}
		} else {
			System.err.println("This program expects data from the TPC-H benchmark as input data.\n" +
								"  Due to legal restrictions, we can not ship generated data.\n" +
								"  You can find the TPC-H data generator at http://www.tpc.org/tpch/.\n" + 
								"  Usage: TPCHQuery10 <customer-csv path> <orders-csv path> <lineitem-csv path> <nation-csv path> <result path>");
			return false;
		}
		return true;
	}
	
	private static DataSet<Tuple5<Integer, String, String, Integer, Double>> getCustomerDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(customerPath)
					.fieldDelimiter('|')
					.includeFields("11110100")
					.types(Integer.class, String.class, String.class, Integer.class, Double.class);
	}
	
	private static DataSet<Tuple3<Integer, Integer, String>> getOrdersDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(ordersPath)
					.fieldDelimiter('|')
					.includeFields("110010000")
					.types(Integer.class, Integer.class, String.class);
	}

	private static DataSet<Tuple4<Integer, Double, Double, String>> getLineitemDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(lineitemPath)
					.fieldDelimiter('|')
					.includeFields("1000011010000000")
					.types(Integer.class, Double.class, Double.class, String.class);
	}
	
	private static DataSet<Tuple2<Integer, String>> getNationsDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(nationPath)
					.fieldDelimiter('|')
					.includeFields("1100")
					.types(Integer.class, String.class);
	}
			
}
