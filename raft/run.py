import subprocess
import p_tqdm
import sys
import time


def test(part):
    command = ["go", "test", "-run", part, "-race"]
    process = subprocess.run(
        args=command,
        # stdout=subprocess.DEVNULL,
    )

    return process.returncode


def sequential(part, tests, d):
    for _ in range(tests):
        returncode = test(part)
        d[returncode] += 1
        print(f"Passed: {d[0]}, Failed: {d[1]}")
    return


def parallel(part, tests, d):
    for returncode in p_tqdm.p_uimap(test, [part] * tests):
        d[returncode] += 1
        print(f"Passed: {d[0]}, Failed: {d[1]}")
    return


if __name__ == "__main__":
    part = sys.argv[1]  # "2B"
    tests = int(sys.argv[2])  # multiple of 8 preferably

    d = {0: 0, 1: 0}

    start = time.time()
    # sequential(part, tests, d)
    parallel(part, tests, d)
    end = time.time()

    print(
        f"\n[{part}] Passed: {d[0]}/{tests}"
        f"\n[{part}] Failed: {d[1]}/{tests}"
        f"\n[{part}] Average time: {(end-start)/tests}"
    )
