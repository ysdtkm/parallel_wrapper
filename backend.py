#!/usr/bin/env python3
# ref: a50p24

import sys, os
import subprocess as sp
import itertools, functools
from multiprocessing import Pool, cpu_count
from frontend import get_output_obj, get_failed_obj
import numpy as np

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

def __inverse_itertools_kd_product(k, params, map_result):
    assert k == len(params)
    ns = [len(params[j]) for j in range(k)]
    assert np.prod(ns, dtype=int) == len(map_result)
    res_np = np.array(map_result, dtype=object)
    res = res_np.reshape(ns)
    return res

def __shell(cmd, writeout=False):
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

def __exec_single_job(wdir_base, k, fmts, changes, command, param):
    assert len(param) == k
    s = [(fmts[j] % param[j]).replace(".", "_") for j in range(k)]
    supp = "".join([f"/{s[j]}" for j in range(k)])
    dname = f"{wdir_base}/wk_parallel{supp}"
    __shell(f"mkdir -p {dname}")
    os.chdir(dname)
    __shell(f"cp -rf {wdir_base}/template_parallel/* .")

    for j in range(k):
        for c in changes[j]:
            c.rewrite_file_with_param(param[j])
    try:
        __shell(command, writeout=True)
        res = get_output_obj()
        print(f"util_parallel: {supp} done")
    except:
        res = get_failed_obj()
        print(f"util_parallel: {supp} failed")
    return res

def exec_parallel(wdir_base, k, params, fmts, changes, command, max_proc=100):
    assert k == len(params) == len(fmts) == len(changes)
    __shell(f"rm -rf {wdir_base}/wk_parallel")
    params_prod = itertools.product(*params)
    job = functools.partial(__exec_single_job, wdir_base, k, fmts, changes, command)
    with Pool(min(cpu_count(), max_proc)) as p:
        res = p.map(job, params_prod)
    return __inverse_itertools_kd_product(k, params, res)

