# TP5 – Spark GraphFrames: Bike Trip Graph Analysis

## Overview
This practical assignment introduces graph processing with Spark GraphX and the GraphFrames library. It consists of modeling a bike-sharing system as a directed graph, where stations are vertices and bike trips are edges, then querying it with DataFrame operations, building subgraphs, and searching for structural motifs.

## Technologies
* Apache Spark (Spark SQL)
* PySpark (Python)
* GraphFrames
* DataFrame API

## Dataset
The dataset consists of two CSV files:

| File | Content |
|------|---------|
| `station_data.csv` | Bike stations (the `name` column identifies each station) |
| `trip_data.csv` | Bike trips, with `Start Station`, `End Station`, and `Duration` among its columns |

The data files are not included in this repository.

## Project Structure
```
.
├── tp5_graphframes.py   # PySpark GraphFrames program
└── README.md
```

## Graph Model
The graph is directed and points from the start of a trip to its end:

* **Vertices**: stations, with the `name` column renamed to `id`.
* **Edges**: trips, with `Start Station` renamed to `src` and `End Station` renamed to `dst`.

## Implementation
The program follows the steps of the assignment:

| # | Task | Technique |
|---|------|-----------|
| 1 | Create a DataFrame for each file | `spark.read.csv` |
| 2–3 | Rename columns to the GraphFrames conventions | `withColumnRenamed` |
| 4 | Build the graph | `GraphFrame(vertices, edges)` |
| 5 | Number of trips between each source and destination, descending | `groupBy("src", "dst").count()` |
| 6 | Trips starting or ending at `Townsend at 7th`, descending | `filter` + `groupBy` |
| 7 | Stations never reached by a trip starting at `Spear at Folsom` | `left_anti` join |
| 8 | Station with the most incoming trips | `inDegrees` |
| 9 | Trip with the longest duration | `orderBy(desc("Duration"))` |
| 10 | Subgraph of trips starting or ending at `Townsend at 7th` | Filtered edges + new `GraphFrame` |
| 11 | Triangle motif between three stations | `find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")` |
| 12 | Paths through three vertices starting at `Townsend at 7th` | `find("(a)-[e1]->(b); (b)-[e2]->(c)")` |

## Execution
GraphFrames is an external Spark package, so it must be provided at submission time. Use the GraphFrames version that matches your Spark and Scala versions:
```bash
spark-submit --packages graphframes:graphframes:<version> tp5_graphframes.py
```
Place the CSV files at the paths used in the script (`file:///root/tp5/`) or adjust them.

## Learning Outcomes
* Modeling relational data as a property graph (vertices and edges).
* Building directed graphs with GraphFrames from DataFrames.
* Querying graphs with DataFrame operations and built-in measures such as `inDegrees`.
* Extracting subgraphs.
* Finding structural patterns (motifs) with the GraphFrames pattern-matching language.

## Academic Context
Practical Work 5 – Big Data
Higher School of Computer Science (ESI-SBA), Algeria
Academic Year: 2026–2027