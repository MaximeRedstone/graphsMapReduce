from itertools import cycle

list2 = [1, 2, 3]
list1 = [9] * len(list2)
zipped = zip(list1, list2)
print(zipped)
for couple in zipped:
    print(f"{couple[0]} {couple[1]}")