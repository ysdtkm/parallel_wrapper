#!/usr/bin/env python3

import collections
import functools
import itertools
from multiprocessing import Pool, cpu_count
import os
import sys
import subprocess as sp
import numpy as np

def main_parallel():
    k = 2
    params = [
        Param("first", "%02d", [1, 2, 3], [Rewrite("echo_numbers.sh", 2, "x", "x={p:02d}")]),
        Param("second", "%.02f", [11.0, 12.0], [Rewrite("echo_numbers.sh", 3, "y", "y={p:f}")])
    ]
    res = Runner.exec_parallel(
        "/home/tak/prgm/parallel_wrapper", k, params, "sh echo_numbers.sh", max_proc=10)
    visualize_results(res)

def visualize_results(res):
    assert isinstance(res, np.ndarray)
    print(res.shape)
    print(res)

def get_output_obj():
    with open("out", "r") as f:
        lines = f.readlines()
    return lines[0]

def get_failed_obj():
    return "Failed"

class Rewrite:
    def __init__(self, file_name, line_num, match, replace_fmt):
        assert isinstance(file_name, str)
        assert isinstance(line_num, int)
        assert isinstance(match, str)
        assert isinstance(replace_fmt, str)
        self.file_name = file_name
        self.line_num = line_num
        self.match = match
        self.replace_fmt = replace_fmt

    def rewrite_file_with_param(self, param):
        with open(self.file_name, "r") as f:
            lines = f.readlines()
        with open(self.file_name, "w") as f:
            for i, l in enumerate(lines):
                if i == self.line_num - 1:
                    assert self.match in l
                    f.write(self.replace_fmt.format(p=param) + "\n")
                else:
                    f.write(l)

class Param:
    def __init__(self, name, path_fmt, values, changes):
        assert isinstance(values, collections.Iterable)
        assert isinstance(changes, collections.Iterable)
        self.name = name
        self.path_fmt = path_fmt
        self.values = values
        self.changes = changes

class Runner:
    @classmethod
    def inverse_itertools_kd_product(cls, k, param_vals, map_result):
        assert k == len(param_vals)
        ns = [len(param_vals[j]) for j in range(k)]
        assert np.prod(ns, dtype=int) == len(map_result)
        res_np = np.array(map_result, dtype=object)
        res = res_np.reshape(ns)
        return res

    @classmethod
    def shell(cls, cmd, writeout=False):
        assert isinstance(cmd, str)
        p = sp.run(cmd, shell=True, encoding="utf8", stderr=sp.PIPE, stdout=sp.PIPE)
        if writeout:
            with open("stdout", "w") as f:
                f.write(p.stdout)
            with open("stderr", "w") as f:
                f.write(p.stdout)
        else:
            if p.returncode != 0:
                print(p.stdout)
                print(p.stderr)
        if p.returncode != 0:
            raise Exception("shell %s failed" % cmd)

    @classmethod
    def exec_single_job(cls, wdir_base, k, params, command, list_param_val):
        assert len(list_param_val) == k
        changes = [p.changes for p in params]
        str_path = [(params[j].path_fmt % list_param_val[j]).replace(".", "_") for j in range(k)]
        supp = "".join([f"/{str_path[j]}" for j in range(k)])
        dname = f"{wdir_base}/wk_parallel{supp}"
        cls.shell(f"mkdir -p {dname}")
        os.chdir(dname)
        cls.shell(f"cp -rf {wdir_base}/template_parallel/* .")

        for j in range(k):
            for c in changes[j]:
                c.rewrite_file_with_param(list_param_val[j])
        try:
            cls.shell(command, writeout=True)
            res = get_output_obj()
            print(f"util_parallel: {supp} done")
        except:
            res = get_failed_obj()
            print(f"util_parallel: {supp} failed")
        return res

    @classmethod
    def exec_parallel(cls, wdir_base, k, params, command, max_proc=100):
        assert k == len(params)
        cls.shell(f"rm -rf {wdir_base}/wk_parallel")
        param_vals = [p.values for p in params]
        param_vals_prod = itertools.product(*param_vals)
        job = functools.partial(cls.exec_single_job, wdir_base, k, params, command)
        with Pool(min(cpu_count(), max_proc)) as p:
            res = p.map(job, param_vals_prod)
        return cls.inverse_itertools_kd_product(k, param_vals, res)

if __name__ == "__main__":
    main_parallel()

