import React, { useEffect, useRef } from "react";
import * as d3 from "d3";

const KafkaNetworkGraphD3Static = () => {
    const svgRef = useRef();

    useEffect(() => {
        // Sample Static Data
        const topic = { id: "my-kafka-topic", type: "topic", partitions: 5 };
        const producers = [
            { id: "producer-1", type: "producer", throughput: 250 },
            { id: "producer-2", type: "producer", throughput: 180 }
        ];
        const consumers = [
            { id: "consumer-1", type: "consumer", lag: 120 },
            { id: "consumer-2", type: "consumer", lag: 3500 },
            { id: "consumer-3", type: "consumer", lag: 700 }
        ];

        // Define Nodes and Links
        const nodes = [topic, ...producers, ...consumers];
        const links = [
            { source: "producer-1", target: "my-kafka-topic" },
            { source: "producer-2", target: "my-kafka-topic" },
            { source: "my-kafka-topic", target: "consumer-1" },
            { source: "my-kafka-topic", target: "consumer-2" },
            { source: "my-kafka-topic", target: "consumer-3" }
        ];

        // Initialize D3
        const svg = d3.select(svgRef.current);
        svg.selectAll("*").remove();

        const width = 800, height = 600;
        const simulation = d3.forceSimulation(nodes)
            .force("link", d3.forceLink(links).id(d => d.id).distance(100))
            .force("charge", d3.forceManyBody().strength(-200))
            .force("center", d3.forceCenter(width / 2, height / 2));

        const tooltip = d3.select("body").append("div")
            .style("position", "absolute")
            .style("visibility", "hidden")
            .style("background", "lightgray")
            .style("padding", "8px")
            .style("border-radius", "5px");

        const link = svg.append("g")
            .selectAll("line")
            .data(links)
            .enter().append("line")
            .attr("stroke", "#999")
            .attr("stroke-width", 2);

        const node = svg.append("g")
            .selectAll("circle")
            .data(nodes)
            .enter().append("circle")
            .attr("r", 20)
            .attr("fill", d => d.type === "topic" ? "blue" : d.lag > 1000 ? "red" : "green")
            .on("mouseover", (event, d) => {
                tooltip.style("visibility", "visible")
                    .html(`<b>${d.id}</b><br>${d.type === "topic" ? `Partitions: ${d.partitions}` : 
                          d.type === "producer" ? `Throughput: ${d.throughput} msgs/sec` : 
                          `Lag: ${d.lag} msgs`}`)
                    .style("top", `${event.pageY + 10}px`)
                    .style("left", `${event.pageX + 10}px`);
            })
            .on("mouseout", () => tooltip.style("visibility", "hidden"));

        simulation.on("tick", () => {
            link.attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            node.attr("cx", d => d.x)
                .attr("cy", d => d.y);
        });

    }, []);

    return <svg ref={svgRef} width={800} height={600}></svg>;
};

export default KafkaNetworkGraphD3Static;
