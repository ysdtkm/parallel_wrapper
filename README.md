# parallel_wrapper: wrapper to sweep a parameter space
## What is done
* Make directories and copy environment
    * In each directory, some of the files can be edited.
* Using multiprocessing.Pool and subprocess.run to run experiments in each directory
* Reduce the results as k-dimentional np.ndarray of objects (inverse_itertools_kd_product)

## Restriction
* The single job must
    * be a shell command
    * write the output to a file

## Requirements
* Python > 3.6 (for subprocess.run and f-string)
* numpy

## Usage
```bash
# put programs-to-be-wrapped into template_parallel/
# edit runner.py
python runner.py
```

## Note
* Basic idea taken from https://github.com/eviatarbach/simulation_runner
* schematic at i04p21
