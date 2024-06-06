"""
Chapter 10

Goal:
Show advanced breadth-first search in PySpark.  Utilizes accumulator

This will perform a breadth-first search on the superhero graph
"""

import os
from enum import Enum
from typing import Tuple, List

from pyspark import SparkConf, SparkContext

SUPERHERO_GRAPH_FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/marvel_graph"
    )
)


DISTANCE_SENTINEL_VALUE = 9999
MAX_DISTANCE = 10


class NodeProcessingState(Enum):
    UNPROCESSED = 1
    PARTIAL = 2
    PROCESSED = 3


START_CHARACTER = 5306
TARGET_CHARACTER = 14


def main():
    def _bfs_map(row):
        hero_id = row[0]
        connections = row[1][0]
        distance = row[1][1]
        color = row[1][2]

        results = []
        if color == NodeProcessingState.PARTIAL.value:
            for connection in connections:
                new_character_id = connection
                new_distance = distance + 1
                new_color = NodeProcessingState.PARTIAL.value
                if TARGET_CHARACTER == connection:
                    hit_counter.add(1)

                new_entry = (new_character_id, ([], new_distance, new_color))
                results.append(new_entry)

            color = NodeProcessingState.PROCESSED.value

        results.append((hero_id, (connections, distance, color)))
        return results

    def _bfs_reduce(row_1, row_2):
        edges_1 = row_1[0]
        edges_2 = row_2[0]
        distance_1 = row_1[1]
        distance_2 = row_2[1]
        processing_state_1 = row_1[2]
        processing_state_2 = row_2[2]

        distance = DISTANCE_SENTINEL_VALUE
        processing_state = NodeProcessingState.UNPROCESSED.value
        edges = []

        if len(edges_1) > 0:
            edges = edges_1
        elif len(edges_2) > 0:
            edges = edges_2

        if distance_1 < distance_2:
            distance = distance_1

        if distance_2 < distance:
            distance = distance_2

        if processing_state_1 == NodeProcessingState.UNPROCESSED.value and (
            processing_state_2 == NodeProcessingState.PROCESSED.value
            or processing_state_2 == NodeProcessingState.PARTIAL.value
        ):
            processing_state = processing_state_2

        if (
            processing_state_1 == NodeProcessingState.PARTIAL.value
            and processing_state_2 == NodeProcessingState.PROCESSED.value
        ):
            processing_state = processing_state_2

        return edges, distance, processing_state

    conf = SparkConf().setMaster("local").setAppName("VFS")
    sc = SparkContext(conf=conf)

    heroes = sc.textFile(SUPERHERO_GRAPH_FILE_LOCATION)

    hit_counter = sc.accumulator(0)

    rdd = heroes.map(_convert_bfs_node)

    for idx in range(0, MAX_DISTANCE):
        mapped = rdd.flatMap(_bfs_map)

        print(f"Processing {mapped.count()} values.")

        if hit_counter.value > 0:
            print(
                f"Hit target character!  From {hit_counter.value} different directions"
            )
            break

        rdd = mapped.reduceByKey(_bfs_reduce)


def _convert_bfs_node(row: str) -> Tuple[int, Tuple[List[int], int, int]]:
    fields = row.split()
    hero_id = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    distance = DISTANCE_SENTINEL_VALUE
    processing_stage = NodeProcessingState.UNPROCESSED.value

    if hero_id == START_CHARACTER:
        processing_stage = NodeProcessingState.PARTIAL.value
        distance = 0

    return hero_id, (connections, distance, processing_stage)


if __name__ == "__main__":
    main()
