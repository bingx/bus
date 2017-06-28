package cn.sibat.createGraph;

/**
 * This class models an undirected Edge in the Graph
 * Created by wing1995 on 2017/6/28.
 */
public class Edge implements Comparable<Edge>{
    private Vertex one, two;
    private int weight;

    /**
     *
     * @param one The first vertex in the Edge
     * @param two The second vertex in the Edge
     */
    public Edge(Vertex one, Vertex two) {
        this(one, two, 1);
    }

    /**
     *
     * @param one The first vertex in the Edge
     * @param two The second vertex of the Edge
     * @param weight The weight of this Edge
     */
    public Edge(Vertex one, Vertex two, int weight) {
        this.one = (one.getLabel().compareTo(two.getLabel()) <= 0) ? one : two;
        this.two = (this.one == one) ? two : one;
        this.weight = weight;
    }

    /**
     *
     * @param current current edge
     * @return The neighbor of current along this edge
     */
    public Vertex getNeighbor(Vertex current) {
        if(!(current.equals(one) || current.equals(two))) {
            return null;
        }
        return (current.equals(one)) ? two : one;
    }

    /**
     *
     * @return Vertex this.one
     */
    public Vertex getOne() {
        return this.one;
    }

    /**
     *
     * @return Vertex this.two
     */
    public Vertex getTwo(){
        return this.two;
    }

    /**
     *
     * @return weight The weight of this Edge
     */
    public int getWeight(){
        return this.weight;
    }

    /**
     *
     * @param weight The new weight of this Edge
     */
    public void setWeight(int weight){
        this.weight = weight;
    }

    /**
     *
     * @param other The Edge to compare against this Edge
     * @return int this.weight - other.weight
     */
    public int compareTo(Edge other){
        return this.weight - other.weight;
    }

    /**
     *
     * @return String A String representation of the Edge
     */
    public String toString() {
        return "({" + one +  ", " + two + "}, " + weight + "})";
    }

    /**
     *
     * @return int The hash code for this Edge
     */
    public int hashCode(){
        return (one.getLabel() + two.getLabel()).hashCode();
    }

    /**
     *
     * @param other The Object to compare against this
     * @return true if other is an Edge with the same Vertex as this
     */
    public boolean equals(Object other){
        if(!(other  instanceof Edge)){
            return false;
        }
        Edge e = (Edge)other;
        return e.one.equals(this.one) && e.two.equals(this.two);
    }
}
