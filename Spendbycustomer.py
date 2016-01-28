# how much each customer spent
from mrjob.job import MRJob

class Spendbycustomer(MRJob):
    def mapper(self,key,line):
        (custID, itemID, price) = line.split(',')
        yield custID, float(price)
    
    def reducer(self, custID, price):
        yield '%04d'%int(custID), sum(price)

if __name__ == '__main__':
    Spendbycustomer.run()

# !python Spendbycustomer.py customer-orders.csv > ordertotals.txt