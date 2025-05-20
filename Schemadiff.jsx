import React, { useEffect, useState } from "react";
import axios from "axios";
import * as d3 from "d3";

const SchemaLineageGraph = () => {
  const [lineageData, setLineageData] = useState(null);

  // Fetch Schema Lineage from API
  useEffect(() => {
    axios.get("/api/schema-lineage/orders") // Change topic dynamically if needed
      .then(response => setLineageData(response.data))
      .catch(error => console.error("Error fetching schema lineage:", error));
  }, []);

  useEffect(() => {
    if (!lineageData) return;

    const width = 800, height = 400;
    const svg = d3.select("#lineageGraph").attr("width", width).attr("height", height);

    const tooltip = d3.select("body").append("div")
      .style("position", "absolute")
      .style("background", "#f9f9f9")
      .style("border", "1px solid #ccc")
      .style("padding", "5px")
      .style("visibility", "hidden");

    const nodes = lineageData.schemas.map(schema => ({
      id: schema.version,
      label: `Schema ${schema.version}`,
      breaking: schema.breakingChange ? "red" : "green",
      fields: schema.fields,
      previousVersion: schema.previousVersion || null
    }));

    const links = lineageData.schemas.slice(1).map((schema, i) => ({
      source: lineageData.schemas[i].version,
      target: schema.version
    }));

    const simulation = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(links).id(d => d.id).distance(100))
      .force("charge", d3.forceManyBody().strength(-200))
      .force("center", d3.forceCenter(width / 2, height / 2));

    const node = svg.selectAll("circle")
      .data(nodes)
      .enter()
      .append("circle")
      .attr("r", 20)
      .attr("fill", d => d.breaking)
      .on("mouseover", (event, d) => {
        let diffText = `Schema ${d.id}\nFields: ${d.fields.join(", ")}`;
        if (d.previousVersion) {
          const prevSchema = nodes.find(n => n.id === d.previousVersion);
          const addedFields = d.fields.filter(f => !prevSchema.fields.includes(f));
          const removedFields = prevSchema.fields.filter(f => !d.fields.includes(f));
          
          diffText += `\nAdded: ${addedFields.join(", ")}`;
          diffText += `\nRemoved: ${removedFields.join(", ")}`;
        }

        tooltip.style("visibility", "visible")
               .style("top", `${event.pageY + 10}px`)
               .style("left", `${event.pageX + 10}px`)
               .text(diffText);
      })
      .on("mouseout", () => tooltip.style("visibility", "hidden"));

    simulation.on("tick", () => {
      node.attr("cx", d => d.x).attr("cy", d => d.y);
      svg.selectAll("line").data(links).enter()
        .append("line")
        .style("stroke", "#999")
        .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
    });

  }, [lineageData]);

  return lineageData ? <svg id="lineageGraph"></svg> : <p>Loading schema lineage...</p>;
};

export default SchemaLineageGraph;
