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
    params = [
        ParameterAxis("first", [1, 2, 3], "%02d", [Rewriter("echo_numbers.sh", 2, "x", "x={param:02d}")]),
        ParameterAxis("second", [11.0, 12.0], "%.02f", [Rewriter("echo_numbers.sh", 3, "y", "y={param:f}")])
    ]
    res = Runner.run_parallel(
        "/home/tak/prgm/parallel_wrapper", params, "sh echo_numbers.sh")
    visualize_results(params, res)

def visualize_results(params, res):
    assert isinstance(res, np.ndarray)
    print("\nvisualization:")
    for ip, p in enumerate(params):
        print(f"{ip}-th parameter '{p.name}' with length {len(p.values)}")
    print(res.shape)
    print(res)

def get_output_obj():
    with open("out", "r") as f:
        lines = f.readlines()
    return lines[0]

def get_failed_obj():
    return "Failed"

# Following are backend. Generally no need to edit.
class Rewriter:
    def __init__(self, relative_file_name, line_num, match, formattable_replacer):
        assert isinstance(relative_file_name, str)
        assert isinstance(line_num, int)
        assert isinstance(match, str)
        assert isinstance(formattable_replacer, str)
        self.relative_file_name = relative_file_name
        self.line_num = line_num
        self.match = match
        self.formattable_replacer = formattable_replacer

    def rewrite_file_with_param(self, param):
        with open(self.relative_file_name, "r") as f:
            lines = f.readlines()
        with open(self.relative_file_name, "w") as f:
            for i, l in enumerate(lines):
                if i == self.line_num - 1:
                    assert self.match in l
                    f.write(self.formattable_replacer.format(param=param) + "\n")
                else:
                    f.write(l)

class ParameterAxis:
    def __init__(self, name, values, path_fmt, rewriters):
        assert isinstance(values, collections.Iterable)
        assert isinstance(rewriters, collections.Iterable)
        self.name = name
        self.path_fmt = path_fmt
        self.values = values
        self.rewriters = rewriters

class Runner:
    @classmethod
    def inverse_itertools_kd_product(cls, param_vals, map_result):
        k_dim = len(param_vals)
        ns = [len(param_vals[j]) for j in range(k_dim)]
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
    def exec_single_job(cls, wdir_base, params, command, list_param_val):
        k_dim = len(params)
        assert len(list_param_val) == k_dim
        str_path_part = [(params[j].path_fmt % list_param_val[j]).replace(".", "_") for j in range(k_dim)]
        suffix_path = "".join([f"/{str_path_part[j]}" for j in range(k_dim)])
        single_dir_name = f"{wdir_base}/wk_parallel{suffix_path}"
        cls.shell(f"mkdir -p {single_dir_name}")
        os.chdir(single_dir_name)
        cls.shell(f"cp -rf {wdir_base}/template_parallel/* .")
        for j in range(k_dim):
            for r in params[j].rewriters:
                r.rewrite_file_with_param(list_param_val[j])
        try:
            cls.shell(command, writeout=True)
            res = get_output_obj()
            print(f"util_parallel: {suffix_path} done")
        except:
            res = get_failed_obj()
            print(f"util_parallel: {suffix_path} failed")
        return res

    @classmethod
    def run_parallel(cls, wdir_base, params, command, max_proc=10):
        cls.shell(f"rm -rf {wdir_base}/wk_parallel")
        param_vals = [p.values for p in params]
        param_vals_prod = itertools.product(*param_vals)
        job = functools.partial(cls.exec_single_job, wdir_base, params, command)
        with Pool(min(cpu_count(), max_proc)) as p:
            res = p.map(job, param_vals_prod)
        return cls.inverse_itertools_kd_product(param_vals, res)

if __name__ == "__main__":
    main_parallel()

