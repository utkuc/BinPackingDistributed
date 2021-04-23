""" Credit to : https://stackoverflow.com/a/7392364 """

class Bin(object):
    """ Container for items that keeps a running sum """
    def __init__(self):
        self.items = []
        self.sum = 0

    def append(self, item):
        self.items.append(item)
        self.sum += item

    def __str__(self):
        """ Printable representation """
        return 'Bin(sum=%d, items=%s)' % (self.sum, str(self.items))


def pack(values, maxValue):
    values = sorted(values, reverse=True)
    bins = []

    for item in values:
        # Try to fit item into a bin
        for bin in bins:
            if bin.sum + item <= maxValue:
                #print 'Adding', item, 'to', bin
                bin.append(item)
                break
        else:
            # item didn't fit into any bin, start a new bin
            #print 'Making new bin for', item
            bin = Bin()
            bin.append(item)
            bins.append(bin)

    return bins


if __name__ == '__main__':
    import random

    def packAndShow(aList, maxValue):
        """ Pack a list into bins and show the result """
        print('List with sum', sum(aList), 'requires at least', (sum(aList) + maxValue - 1) / maxValue, 'bins')

        bins = pack(aList, maxValue)

        print('Solution using', len(bins), 'bins:')
        for bin in bins:
            print(bin)

        print




    aList = [ random.randint(1, 130) for i in range(50000) ]
    packAndShow(aList, 130)