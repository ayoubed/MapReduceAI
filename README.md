The current program implements a map reduce system with custom inputs and outputs, trying policies and backoff mechanism.
It relies on a topological sort to build the graph of the tasks that need to be executed.

the main includes a simples example of text parsing and translation

text1 -> parse1 -> translate1 \
                              -> merge
text2 -> parse2 -> translate2 /