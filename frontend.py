#!/usr/bin/env python3

from functools import partial
import subprocess as sp
import numpy as np

def wrap_parallel():
    from backend import Rewrite, exec_parallel
    k = 2
    params = [[1, 2, 3], [11.0, 12.0]]
    fmts = ["%02d", "%.02f"]
    changes = [[Rewrite("echo_numbers.sh", 2, "x", "x={p:02d}")],
               [Rewrite("echo_numbers.sh", 3, "y", "y={p:f}")]]
    res = exec_parallel("/home/tak/prgm/parallel_wrapper",
                  k, params, fmts, changes, "sh echo_numbers.sh")
    visualize_results(res)

def visualize_results(res):
    assert isinstance(res, np.ndarray)
    print(res.shape)
    print(res)

# for each realization, get_output_obj or get_output_obj is called from backend.py
def get_output_obj():
    with open("out", "r") as f:
        lines = f.readlines()
    return lines[0]

def get_failed_obj():
    return "Failed"

if __name__ == "__main__":
    wrap_parallel()

