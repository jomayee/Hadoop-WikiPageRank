Jyothirmayi Panda 

PageRank directory:

This folder contains four files

PageRank.java --> Main file with all the jobs configuration.
Task0.java --> Nodes count
Task1.java --> Create Initial graph. Takes every line from the file as input and gives output as <source, pagerank#####outlink1&#&#&outlink2...>
Task2.java --> Takes input as <source, pagerank#####outlink1&#&#&outlink2...> 
				Mapper gives two outputs, <outlink, rank> and <source, :outlinklist>
				Reducer calculates the rank using damping factor and writes the output as <outlink, updatedrank#####outlinklist>
Task3.java --> Mapper multiplies the ranks by -1 and write the output as <rank, outlink>. Mapper write the output in descending order due to negative values.
				Reducer receives the output in the descending order, we just shuffle the places of rank, outlink and write the final output as <outlink, rank>

				



