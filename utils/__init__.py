from .neo4j_driver import (
    Neo4jDriver
)

import numpy as np
from numpy.linalg import norm

def get_cosine_sim(a: np.array, b: np.array):
    a_ = np.array(a)
    b_ = np.array(b)
    cos_sim = np.dot(a_, b_)/(norm(a_) * norm(b_))
    return cos_sim

def get_euclidean_dist(a: np.array, b: np.array):
    a_ = np.array(a)
    b_ = np.array(b)
    return norm(a_ - b_)