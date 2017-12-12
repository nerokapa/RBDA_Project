import matplotlib.pyplot as plt
import numpy as np
import ast, os

def get_logged_array(filename):
    with open(filename) as f:
        diffs = []
        goldens = []
        preds = []
        for line in f:
            golden, pred = ast.literal_eval(line)
            if golden is 0 or pred is 0:
                continue
            diffs.append(np.log10(golden/pred))
            goldens.append(golden)
            preds.append(pred)
    return diffs, goldens, preds

def plot(title, data, filename):
    n, bins, patches = plt.hist(np.array(data), bins = 30, range = (-1, 1))
    plt.xlabel('log of gold/predict')
    plt.ylabel('count')
    plt.title(title)
    plt.text(60, .025, r'$\mu=100,\ \sigma=15$')
    plt.axis([-1, 1, 0, 120])
    plt.grid(True)
    plt.savefig(filename)

if __name__ == "__main__":
    dirs = filter(os.path.isdir, os.listdir("./"))
    for d in dirs:
        files = filter(lambda x: x.split(".")[-1] == "dat", os.listdir(d))
        for f in files:
            title = f.split("_")[0]
            path = d+"/"+f
            diffs, goldens, preds = get_logged_array(path)
            plot(title, diffs, d+"/"+f.replace("dat","png"))
