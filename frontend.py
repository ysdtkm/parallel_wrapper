#!/usr/bin/env python3

import numpy as np
from functools import partial
import subprocess as sp

def wrap_parallel():
    from backend import Rewrite, exec_parallel

    k = 3
    params = [[1, 2, 3], [11.0, 12.0, 13.0], [5, 6]]
    fmts = ["%02d", "%.02f", "%02d"]
    changes = [[Rewrite("echo_numbers.sh", 2, "x", "x={p:02d}")],
               [Rewrite("echo_numbers.sh", 3, "y", "y={p:f}")],
               []]

    res = exec_parallel("/mnt/c/repos/works/2018/parallel_wrapper",
                  k, params[:k], fmts[:k], changes[:k], "sh echo_numbers.sh")
    visualize_results(res)

def get_output_obj():
    with open("out", "r") as f:
        lines = f.readlines()
    return lines[0]

def get_failed_obj():
    return "Failed"

def visualize_results(res):
    assert isinstance(res, np.ndarray)
    print(res.shape)
    print(res)

if __name__ == "__main__":
    wrap_parallel()

