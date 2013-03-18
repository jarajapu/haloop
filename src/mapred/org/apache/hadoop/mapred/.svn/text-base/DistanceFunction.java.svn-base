/**
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

package org.apache.hadoop.mapred;

import java.util.Set;

/**
 * application developers need to implement this class to define the distance
 * for convergence checking
 * 
 * @author ybu
 *
 * @param <Value> the resulting Value class
 */
public interface DistanceFunction<Value> {

	/**
	 * the result distance between two value sets, which have the same key
	 * @param v1, value set from preceding iteration
	 * @param v2, value set from consequent iteration
	 * @return double valued distance
	 */
	public double resultDistance(Set<Value> v1, Set<Value> v2);
}
