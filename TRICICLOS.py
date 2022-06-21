import json
from multiprocessing import Process, Manager
import os

class Graph:
    DIRECTED = False

    def __init__(self):
        """
        Initialize a graph.
        """
        self.node_neighbors = {}
    
    def nodes(self):
        """
        Return node list.
        """
        return list(self.node_neighbors.keys())

    def neighbors(self, node):
        """
        Return all nodes that are directly accessible from given node.
        """
        return list(self.node_neighbors[node])
    
    def edges(self):
        """
        Return all edges in the graph.
        """
        return self.node_neighbors.items()

    def has_node(self, node):
        """
        Return whether the requested node exists.
        """
        return node in self.node_neighbors

    def add_node(self, node):
        """
        Add given node to the graph.
        """
        if (not node in self.node_neighbors):
            self.node_neighbors[node] = set()
        else:
            raise Exception("Node %s already in graph" % node)

    def add_edge(self, edge):
        """
        Add an edge to the graph connecting two nodes.
        An edge, here, is a pair of nodes like C{(n, m)}.
        """
        u, v = edge
        if (v not in self.node_neighbors[u] and u not in self.node_neighbors[v]):
            self.node_neighbors[u].add(v)
            if (u != v):
                self.node_neighbors[v].add(u)
        else:
            raise Exception("Edge (%s, %s) already in graph" % (u, v))

    def del_node(self, node):
        """
        Remove a node from the graph.
        """
        for each in list(self.neighbors(node)):
            if (each != node):
                self.del_edge((each, node))
        del(self.node_neighbors[node])

    def del_edge(self, edge):
        """
        Remove an edge from the graph.
        """
        u, v = edge
        self.node_neighbors[u].remove(v)
        if (u != v):
            self.node_neighbors[v].remove(u)
    
    def __str__(self):
        """
        Return a string representing the graph when requested by str() (or print).
        """
        str_nodes = repr( self.nodes() )
        str_edges = repr( self.edges() )
        return "%s %s" % ( str_nodes, str_edges )

    def __repr__(self):
        """
        Return a string representing the graph when requested by repr()
        """
        return "<%s>" % ( str(self) )
            
    def __len__(self):
        """
        Return the order of self when requested by len().
        """
        return self.order()
            
    def add_nodes(self, nodelist):
        """
        Add given nodes to the graph.
        """
        for each in nodelist:
            self.add_node(each)

class Solver:
    def __init__(self, graph, target_length = 3):
        """
        Initialize a solver.
        """
        self.length = target_length
        self.graph = graph
        self.visited = {}
        self.spanning_tree = {}
        self.cycle = []
        self.cycles = []

    def find_cycle_to_ancestor(self, node, ancestor):
        """
        Find a cycle containing both node and ancestor.
        Use multiprocessing to speed up the search.
        """
        path = []
        while (node != ancestor):
            if (node is None):
                return []
            path.append(node)
            node = self.spanning_tree[node]
        path.append(node)
        path.reverse()
        return path
    
    def dfs(self, node):
        """
        Depth-first search subfunction.
        """
        self.visited[node] = 1

        # Explore recursively the connected component
        for each in self.graph.neighbors(node):
            if (self.cycle):
                return
            if (each not in self.visited):
                self.spanning_tree[each] = node
                self.dfs(each)
            else:
                if (self.spanning_tree[node] != each):
                    self.cycle.extend(self.find_cycle_to_ancestor(node, each))
        
    def find_cycle(self):
        for each in self.graph.nodes():
            # Select a non-visited node
            if (each not in self.visited):
                self.spanning_tree[each] = None

                # Explore node's connected component
                self.dfs(each)
                if (self.cycle):
                    self.cycles.append(self.cycle) # Save the cycle found
                    self.cycle = []           # Reset cycle
                    self.spanning_tree = {}   # Reset spanning tree
                    self.visited = {}         # Reset visited nodes

        # Remove duplicates
        return set(map(tuple, [ list(set(cycle)) for cycle in self.cycles if len(cycle) == self.length ]))


def find_cycles(graph, length = 3):
    """
    Find all the N-cycles (N=length) in the given graph.
    """
    solver = Solver(graph, length)
    return solver.find_cycle()


def process_file(filename, return_dict):
    """
    Worker function for multiprocessing.
    """
    print(f"Processing file {filename}") 
    return_dict[filename] = find_cycles(parse_graph_file(filename), 3)


def parse_graph_file(filename):
    """
    Parse a graph file and return a graph object.
    """

    nodes = []
    with open(filename) as fp:
        edges = json.load(fp)
        edges = [ (u,v) for u,v in edges ]

        print("Parsing graph file %s" % filename)
        print("Edges: ", edges)

        # Extract nodes
        for u,v in edges:
            if u not in nodes:
                nodes.append(u)
            if v not in nodes:
                nodes.append(v)
        
        print("Nodes: ", nodes)

        # Create graph
        graph = Graph()
        graph.add_nodes(nodes)

        # Add edges
        for u,v in edges:
            graph.add_edge((u,v))

    return graph


def ejercicio1():
    """
    Ejercicio 1:
    Escribe un programa paralelo que calcule los 3-ciclos de un grafo definido como lista de aristas
    """

    graph = parse_graph_file("data/ejercicio1/grafo_1.json")
    cycles = find_cycles(graph, 3)
    print("\nCYCLES FOUND: ", cycles)


def ejercicio2():
    """
    Ejercicio 2:
    Escribe un programa paralelo que calcule los 3-ciclos de un grafo que se 
    encuentra definido en múltiples ficheros de entrada
    """
    base_dir = "data/ejercicio2/"
    return_dict = Manager().dict()
    files = [os.path.join(base_dir, filename) for filename in os.listdir(base_dir) if filename.endswith(".json")]
    processes = []

    for filename in files:
        # Process each file using a separate process
        p = Process(target=process_file, args=(filename, return_dict))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()

    # Join all the results
    results = [ return_dict[filename] for filename in files ]
    print("\nCYCLES FOUND: ", results)


def ejercicio3():
    """
    Ejercicio 3:
    Escribe un programa paralelo que calcule independientemente los 3-ciclos de cada uno de
    los ficheros de entrada.
    """

    base_dir = "data/ejercicio3/"
    return_dict = Manager().dict()
    files = [os.path.join(base_dir, filename) for filename in os.listdir(base_dir) if filename.endswith(".json")]
    processes = []

    for filename in files:
        # Process each file using a separate process
        p = Process(target=process_file, args=(filename, return_dict))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    # Join all the results
    print("\nCYCLES FOUND: ", return_dict)


def main():

    print("<EJERCICIO 1>")
    ejercicio1()
    print("<\EJERCICIO 1>\n\n")

    print("<EJERCICIO 2>")
    ejercicio2()
    print("<\EJERCICIO 2>\n\n")

    print("<EJERCICIO 3>")
    ejercicio3()
    print("<\EJERCICIO 3>")

if __name__ == "__main__":
    main()