from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# Spark session with the GraphFrames package
# The jar is downloaded automatically on first run.
# ============================================================
spark = SparkSession.builder \
    .appName("TP5-GraphFrames") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

from graphframes import GraphFrame


def title(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


# ============================================================
# 1) Create a DataFrame for each file
# ============================================================
stations = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("file:///root/tp5/station_data.csv")

trips = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("file:///root/tp5/trip_data.csv")

title("(1) DataFrames created")
print("Stations rows:", stations.count())
print("Trips rows   :", trips.count())
stations.show(3, truncate=False)
trips.show(3, truncate=False)


# ============================================================
# 2) Vertices table: rename 'name' -> 'id'
#    (GraphFrames requires the vertex identifier column
#     to be called exactly 'id')
# ============================================================
vertices = stations.withColumnRenamed("name", "id").distinct()

title("(2) Vertices (id = station name)")
vertices.select("id").show(5, truncate=False)


# ============================================================
# 3) Edges table: rename 'Start Station' -> 'src'
#                 and    'End Station'   -> 'dst'
#    (GraphFrames requires edge columns 'src' and 'dst')
# ============================================================
edges = trips \
    .withColumnRenamed("Start Station", "src") \
    .withColumnRenamed("End Station", "dst")

title("(3) Edges (src -> dst)")
edges.select("src", "dst", "Duration").show(5, truncate=False)


# ============================================================
# 4) Create the GraphFrame (directed graph: src -> dst)
# ============================================================
g = GraphFrame(vertices, edges)

title("(4) GraphFrame created")
print("Number of vertices:", g.vertices.count())
print("Number of edges   :", g.edges.count())


# ============================================================
# 5) Number of trips between each (src, dst), descending
# ============================================================
title("(5) Trips per (src, dst), descending")
g.edges.groupBy("src", "dst") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(10, truncate=False)


# ============================================================
# 6) Trips starting OR ending at 'Townsend at 7th', descending
# ============================================================
title("(6) Trips starting or ending at 'Townsend at 7th'")
g.edges \
    .filter((F.col("src") == "Townsend at 7th") |
            (F.col("dst") == "Townsend at 7th")) \
    .groupBy("src", "dst") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(10, truncate=False)


# ============================================================
# 7) Vertices that were NEVER a destination of a trip
#    starting from 'Spear at Folsom'
# ============================================================
title("(7) Stations never reached from 'Spear at Folsom'")

reached = g.edges \
    .filter(F.col("src") == "Spear at Folsom") \
    .select("dst") \
    .distinct()

never_reached = g.vertices.join(
    reached,
    g.vertices["id"] == reached["dst"],
    "left_anti"          # keep vertices NOT present in 'reached'
)

never_reached.select("id").show(50, truncate=False)
print("Count:", never_reached.count())


# ============================================================
# 8) Station with the maximum number of INCOMING trips
#    (inDegrees = number of edges arriving at each vertex)
# ============================================================
title("(8) Station with max incoming trips")
g.inDegrees \
    .orderBy(F.desc("inDegree")) \
    .show(1, truncate=False)


# ============================================================
# 9) Trip with the longest duration
# ============================================================
title("(9) Trip with the longest duration")
g.edges \
    .withColumn("Duration", F.col("Duration").cast("long")) \
    .orderBy(F.desc("Duration")) \
    .show(1, truncate=False)


# ============================================================
# 10) Subgraph: only trips starting or ending at 'Townsend at 7th'
# ============================================================
title("(10) Subgraph around 'Townsend at 7th'")

sub_edges = g.edges.filter(
    (F.col("src") == "Townsend at 7th") |
    (F.col("dst") == "Townsend at 7th")
)

# Keep only the vertices that appear in the kept edges
used_ids = sub_edges.select(F.col("src").alias("id")) \
    .union(sub_edges.select(F.col("dst").alias("id"))) \
    .distinct()

sub_vertices = g.vertices.join(used_ids, "id")

subgraph = GraphFrame(sub_vertices, sub_edges)

print("Subgraph vertices:", subgraph.vertices.count())
print("Subgraph edges   :", subgraph.edges.count())
subgraph.edges.select("src", "dst").show(5, truncate=False)


# ============================================================
# 11) Motif: triangle between three stations
#     a -> b -> c -> a
# ============================================================
title("(11) Triangle motif (a)->(b)->(c)->(a)")

triangles = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)") \
    .filter("a.id != b.id AND b.id != c.id AND c.id != a.id")

triangles.select("a.id", "b.id", "c.id").show(10, truncate=False)
print("Number of triangle paths:", triangles.count())


# ============================================================
# 12) Paths through three vertices starting at 'Townsend at 7th'
#     a -> b -> c  with a = 'Townsend at 7th'
# ============================================================
title("(12) Paths a->b->c starting at 'Townsend at 7th'")

paths = g.find("(a)-[e1]->(b); (b)-[e2]->(c)") \
    .filter(F.col("a.id") == "Townsend at 7th") \
    .filter("a.id != b.id AND b.id != c.id")

paths.select("a.id", "b.id", "c.id").show(10, truncate=False)
print("Number of paths:", paths.count())


print("\nTP5 completed successfully.")
spark.stop()