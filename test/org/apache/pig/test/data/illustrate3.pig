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

A = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:int);
A1 = group A by x;
A2 = foreach A1 generate group, COUNT(A);
store A2 into 'A';
B = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:double, y:int);
B1 = group B by x;
B2 = foreach B1 generate group, COUNT(B);
store B2 into 'B';
