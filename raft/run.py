import subprocess
import os
import p_tqdm


def test(part):
    command = ["go", "test", "-run", part, "-race"]
    process = subprocess.run(args=command, stdout=subprocess.DEVNULL)

    return process.returncode


if __name__ == "__main__":
    part = "2B"
    tests = 50

    d = {0: 0, 1: 0}
    for returncode in p_tqdm.p_uimap(test, [part] * tests):
        d[returncode] += 1
        os.system("clear")
        print(f"Passed: {d[0]}, Failed: {d[1]}")

    print(f"\n[{part}] Passed: {d[0]}/{tests}\n[{[part]}] Failed: {d[1]}/{tests}")
