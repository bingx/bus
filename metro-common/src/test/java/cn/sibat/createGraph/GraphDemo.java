package cn.sibat.createGraph;

/**
 * Graph demo
 * Created by wing1995 on 2017/6/28.
 */
public class GraphDemo {
    public static void main(String[] args){
        Graph graph = new Graph();

        //Initialize some vertices and add them to the graph
        Vertex[] vertices = new Vertex[5];
        for(int i = 0; i < vertices.length; i++){
            vertices[i] = new Vertex("" + i);
            graph.addVertex(vertices[i], true);
        }

        for(int i = 0; i < vertices.length - 1; i++){
            for(int j = i + 1; j < vertices.length; j++){
                graph.addEdge(vertices[i], vertices[j]);
            }
        }

        //Display the initial setup all vertices adjacent to each other
        for (Vertex vertice : vertices) {
            System.out.println(vertice);
            for (Edge neighbor : vertice.getNeighbors()){
                System.out.println(neighbor);
            }
        }
            System.out.println();

        //overwrite Vertex 3
        graph.addVertex(new Vertex("3"), true);
        for(int i = 0; i < vertices.length; i++){
            System.out.println(vertices[i]);

            for(int j = 0; j < vertices[i].getNeighborCount(); j++){
                System.out.println(vertices[i].getNeighbor(j));
            }

            System.out.println();
        }

        System.out.println("Vertex 5: " + graph.getVertex("5"));
        System.out.println("Vertices 3: " + graph.getVertex("3"));

        //true
        System.out.println("Graph Contains {1, 2}: " +
                graph.containsEdge(new Edge(graph.getVertex("1"), graph.getVertex("2"))));

        System.out.println(graph.removeEdge(new Edge(graph.getVertex("1"), graph.getVertex("2"))));

        //false
        System.out.println("Graph Contains {1, 2}: " + graph.containsEdge(new Edge(graph.getVertex("1"), graph.getVertex("2"))));

        //false
        System.out.println("Graph Contains {2, 3} " + graph.containsEdge(new Edge(graph.getVertex("2"), graph.getVertex("3"))));

        System.out.println(graph.containsVertex(new Vertex("1"))); //true
        System.out.println(graph.containsVertex(new Vertex("6"))); //false
        System.out.println(graph.removeVertex("2")); //Vertex 2
        System.out.println(graph.vertexKeys()); //[3, 1, 0, 4]
   }
}
