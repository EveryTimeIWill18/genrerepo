#!/usr/bin/env python2.7

"""
Graph
=====
The Graph module contains the graph data structure that
lays the foundation of the data pipeline.

"""
from collections import deque
from abc import ABCMeta, abstractmethod

class BaseDAG:
    """
    BaseDAG
    ------
    Abstract Graph Interface.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def add(cls,
            node,      # type: int
            to=None    # type: None, int
            ):
        # type: (...) -> None
        """
        Adds node to the graph.

        :param node:
        :param to:
        :return: None
        """
        pass

    @abstractmethod
    def in_degrees(cls):
        # type: (...) -> dict
        """
        Calculates the number of vertices
        between nodes.

        :return: dict
        """
        pass

    @abstractmethod
    def sort(cls):
        # type: (...) -> list
        """
        Topological sort.

        :return: list
        """
        pass


class DirectedGraph(BaseDAG):

    def __init__(self):
        BaseDAG.__init__(self)
        self.graph = {}

    def add(self, node, to=None):
        if node not in self.graph:
            self.graph[node] = []
        if to:
            if not to in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        if len(self.sort()) != len(self.graph):
            raise Exception

    def in_degrees(self):
        """
        :return: in_degrees
        """
        in_degrees = {}
        for node in self.graph:
            if node not in in_degrees:
                in_degrees[node] = 0
            for pointed in self.graph[node]:
                if pointed not in in_degrees:
                    in_degrees[pointed] = 0
                in_degrees[pointed] += 1
        return in_degrees

    def sort(self):
        in_degrees = self.in_degrees()
        to_visit = deque()
        for node in self.graph:
            if in_degrees[node] == 0:
                to_visit.append(node)

        searched = []
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
        return searched


