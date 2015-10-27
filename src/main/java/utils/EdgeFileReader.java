package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * @author Hooman
 */
public class EdgeFileReader {

    private final String DEFAULT_DELIMITER = "\t";
    private final String COMMENT = "#";
    private final String delimiter;

    public EdgeFileReader() {
        this.delimiter = DEFAULT_DELIMITER;
    }

    public EdgeFileReader(String delimiter) {
        this.delimiter = delimiter;
    }

    public Collection<Tuple3<Long, Long, Double>> read(String file) {
        // TODO: investigate about the ordering of edges.
        HashSet<Long> vertices = new HashSet<>();
        HashSet<Tuple3<Long, Long, Double>> edges = new HashSet<>();
        HashMap<Long, Integer> degrees = new HashMap<>();
        int nEdges = 0;
        int nVertices = 0;
        try {
            FileInputStream fis = new FileInputStream(new File(file));
            InputStreamReader isr = new InputStreamReader(fis);
            try (BufferedReader in = new BufferedReader(isr)) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith(COMMENT)) {
                        continue;
                    } //skip comments
                    String values[] = line.split(delimiter);
                    long u = Long.parseLong(values[0]);
                    long v = Long.parseLong(values[1]);
                    if (u != v) {
                        Tuple3<Long, Long, Double> e = new Tuple3(u, v, 1.0) {

                            @Override
                            public boolean equals(Object o) {
                                if (this == o) {
                                    return true;
                                }
                                if (!(o instanceof Tuple3)) {
                                    return false;
                                }

                                final Tuple3<Long, Long, Double> other = (Tuple3<Long, Long, Double>) o;
                                if (!Objects.equals(this.f0, other.f0)) {
                                    return (Objects.equals(this.f0, other.f1)) && (Objects.equals(this.f1, other.f0));
                                }
                                return (Objects.equals(this.f1, other.f1));
                            }

                            @Override
                            public int hashCode() {
                                return toString().hashCode();
                            }

                            @Override
                            public String toString() {
                                String s = "";
                                if ((Long) f0 < (Long) f1) {
                                    s = f0 + "," + f1;
                                } else {
                                    s = f1 + "," + f0;
                                }
                                return s;
                            }

                        };

                        if (edges.add(e)) {
                            nEdges++;
                        }

                        if (vertices.add(u)) {
                            nVertices++;
                        }
                        if (vertices.add(v)) {
                            nVertices++;
                        }

                        if (!degrees.containsKey(u)) {
                            degrees.put(u, 0);
                        }
                        degrees.put(u, degrees.get(u) + 1);
                        if (!degrees.containsKey(v)) {
                            degrees.put(v, 0);
                        }
                        degrees.put(v, degrees.get(v) + 1);
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(-1);
        }

        System.out.println(String.format("Number of vertices: %d", nVertices));
        System.out.println(String.format("Number of edges: %d", nEdges));

        return edges;
    }
}
