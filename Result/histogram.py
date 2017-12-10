import numpy as np
import matplotlib.pyplot as plt

# Fixing random state for reproducibility
# np.random.seed(19680801)
#
# mu, sigma = 100, 15
# x = mu + sigma * np.random.randn(10000)
#
# the histogram of the data
# n, bins, patches = plt.hist(x, 50, normed=1, facecolor='g', alpha=0.75)
x = []
with open('../not_buget.result') as result:
    for i in range(5):
        next(result)
    for row in result:
        # import pdb; pdb.set_trace()
        try:
            num = row[row.rfind(',') + 1 : row.rfind(')')]
            print num
            x.append(float(num))
        except IndexError:
            pass
x = np.array(x)
# import pdb; pdb.set_trace()
print len(x)


# bins = [0, 0.150]
# hist, bin_edges = np.histogram(x, bins=bins)
# import pdb; pdb.set_trace()

n, bins, patches = plt.hist(x, bins = 30, range = (-1, 1))
plt.xlabel('log of gold/predict')
plt.ylabel('Count')
plt.title('log of gold/predict using CastYearGenreLang')
plt.text(60, .025, r'$\mu=100,\ \sigma=15$')
plt.axis([-1, 1, 0, 150])
plt.grid(True)
plt.show()
# plt.save()
