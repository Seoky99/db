The top-level class that reads the input and produces output is Main.

Choice of implementation for each logical selection operator: 
We compare the costs of a full scan versus the cost of each index on a relation, for which for each attribute there can be at most one, with one clustered index overall. For estimating the full scan cost, we appropriate the number of tuples and the bytes it takes up and divide it by the size of a page. For estimating the cost an index: we first calculate a reduction factor based on how of the range of values for an attribute an index narrows down, and for a clustered index, since the data is in the leaves, we assume a 3 page cost of traversing the index plus the reduced pages scanned once we find the index value. For unclustered, we assume the same 3 costs, plus traversing the reduced pages in the associated level, plus a pessimistic “tuple-chasing” cost for each tuple we must process. Then, we compare all the costs and choose the cheapest selection. 
The decision can be found in the logical package with the PhysicalPlanBuilder.java file, under the visit function for a LogicalSelectOperator. 

Criteria for choosing between SMJ and BNLJ: 

Based on our benchmarking in part 2, we concluded that for most scenarios, SMJ runs faster than BNLJ and produces sorted output, which is often useful. Therefore, we want to implement joins with SMJ whenever possible. However, since SMJ can’t evaluate equality conditions or pure cross-products, we will use BLNJ in these scenarios. Additionally, when the tables being joined together are relatively small, the degree of performance difference between SMJ and BNLJ is small as well, and we will utilize BNLJ to avoid writing intermediary results and save some memory. 

This decision can be found in the logical package with the PhysicalPlanBuilder.java file, under the visit function for a LogicalJoinOperator. 

Selection pushing: 
We construct a union-find data structure (found in cs4321 package, UnionFind.java) that has the ability to merge, keep track of upper-lower-equality constraints, and find an element based on attribute value: in order to propagate selection through equalities. We use the visitor (UnionFindVisitor.java in the visitor package) to utilize the usable selections to build such union find from the WHERE expression. Then for each element of the union find, we find the constraints on it and its attributes associated with it, then create an appropriate selection representing the element, and we modify the query plan to put this all under one big multi join operator. 
This can be found in the cs4321 packages, in the QueryPlan.java file. 

Choice in join order:
We essentially test the costs of all possible configurations of joins based on the V-values they generate (using the formula given to us to calculate these V-Values) against each other. We keep track of intermediate join costs for future calls in a data collection, then later iterate through all subsets of size 2 to size (relation set) and find the lowest cost for each plan and the resulting size, each iteration using work we already completed to calculate the work on the current iteration, until we have the complete set of lowest cost. 
This can be found in the cs4321 package, in the file JoinOrderCalculator.java.
