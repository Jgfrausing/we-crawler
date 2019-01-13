import numpy as np
from scipy._lib.six import xrange
from scipy.sparse import csc_matrix

import db


def pageRank(G, s=.85, maxerr=.0001):

    n = G.shape[0]

    # transform G into markov matrix A
    A = csc_matrix(G, dtype=np.float)
    rsums = np.array(A.sum(1))[:, 0]
    ri, ci = A.nonzero()
    A.data /= rsums[ri]

    # bool array of sink states
    sink = rsums == 0

    # Compute pagerank r until we converge
    ro, r = np.zeros(n), np.ones(n)
    while np.sum(np.abs(r - ro)) > maxerr:
        ro = r.copy()
        # calculate each pagerank at a time
        for i in xrange(0, n):
            # inlinks of state i
            Ai = np.array(A[:, i].todense())[:, 0]
            # account for sink states
            Di = sink / float(n)
            # account for teleportation to state i
            Ei = np.ones(n) / float(n)

            r[i] = ro.dot(Ai * s + Di * s + Ei * (1 - s))

    # return normalized pagerank
    return r / float(sum(r))


if __name__ == '__main__':
    domain_refs = db.Database().get_domain_ref()

    domains = dict()
    i = 0
    for x in domain_refs:
        if x[0] not in domains:
            domains[x[0]] = i
            i += 1
        if x[1] not in domains:
            domains[x[1]] = i
            i += 1

    n = len(domains)
    matrix = np.zeros((n, n))

    for x in domain_refs:
        matrix[domains[x[0]], domains[x[1]]] = 1


    pr = pageRank(matrix)

    print(pr)