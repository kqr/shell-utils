import sys


# A simple script to quickly summarise a bunch
# of numbers in a box plot. Works like this:
#
#     $ python st.py
#     4.91
#     3.2
#     5.12
#     3.99
#     8.7
#     <3.20-------[3.99--------|4.91|-5.12]--------------------------------8.70>
#     avg: 5.18
#
# TODO: refactor into a function with a docstring
# TODO: don't do I/O in the function
# TODO: mark outliers specifically? (outliers defined
#       as on http://sv.wikipedia.org/wiki/L%C3%A5dagram )
# TODO: add possibility to choose formatting options with
#       arguments to the script

numbers = [float(line) for line in sys.stdin]
numbers.sort()

quarter = int(len(numbers)/4)

min = numbers[0]
q1 = numbers[1*quarter]
q2 = numbers[2*quarter]
q3 = numbers[3*quarter]
max = numbers[-1]

scaling = 50/(max - min)
len1 = int((q1 - min)*scaling)
len2 = int((q2 - q1)*scaling)
len3 = int((q3 - q2)*scaling)
len4 = int((max - q3)*scaling)

print('<{0:.2f}{1}[{2:.2f}{3}|{4:.2f}|{5}{6:.2f}]{7}{8:.2f}>'.format(min, '-'*len1, q1, '-'*len2, q2, '-'*len3, q3, '-'*len4, max))
print('avg: {0:.2f}'.format(sum(numbers)/len(numbers)))

