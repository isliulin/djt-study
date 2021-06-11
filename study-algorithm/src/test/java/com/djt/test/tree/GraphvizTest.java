package com.djt.test.tree;

import guru.nidi.graphviz.attribute.*;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static guru.nidi.graphviz.model.Factory.*;

/**
 * Graphviz 绘图测试
 *
 * @author 　djt317@qq.com
 * @since 　 2021-06-10
 */
public class GraphvizTest {

    @Test
    public void testGraphviz1() throws IOException {
        Graph g = graph("example1").directed()
                .graphAttr().with(Rank.dir(Rank.RankDir.LEFT_TO_RIGHT))
                .nodeAttr().with(Font.name("arial"))
                .linkAttr().with("class", "link-class")
                .with(node("a").with(Color.RED).link(node("b")),
                        node("b").link(to(node("c")).with(Attributes.attr("weight", 5), Style.DASHED)
                        )
                );
        Graphviz.fromGraph(g).height(100).render(Format.PNG).toFile(new File("src/main/graphviz/ex1.png"));
    }

    @Test
    public void testGraphviz2() throws IOException {
        MutableNode nodeA = mutNode("a");
        MutableNode nodeB = mutNode("b");
        //nodeA.attrs(Label.Justification.LEFT);
        nodeA.add(Color.RED).addLink(nodeB);

        MutableGraph g = mutGraph("example1").setDirected(true);
        g.add(nodeA);

        Graphviz.fromGraph(g).width(200).render(Format.PNG).toFile(new File("src/main/graphviz/ex2.png"));
    }

    @Test
    public void testGraphviz3() throws IOException {
        MutableGraph g = mutGraph("example1").setDirected(true).use((gr, ctx) -> {
            mutNode("b");
            nodeAttrs().add(Color.RED);
            mutNode("a").addLink(mutNode("b"));
        });
        Graphviz.fromGraph(g).width(200).render(Format.PNG).toFile(new File("src/main/graphviz/ex3.png"));
    }

    @Test
    public void testGraphviz4() throws IOException {
        MutableGraph g = mutGraph("example1").setDirected(true).use((gr, ctx) -> {
            MutableNode nodeA = mutNode("a");
            MutableNode nodeB = mutNode("b");
            MutableNode nodeC = mutNode("c");
            nodeA.addLink(nodeC, nodeB);
        });
        Graphviz.fromGraph(g).width(200).render(Format.PNG).toFile(new File("src/main/graphviz/ex4.png"));
    }
}
