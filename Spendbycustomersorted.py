#spend by customer, sort by price
from mrjob.job import MRJob
from mrjob.step import MRStep

class Spendbycustomersorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prices,
                   reducer=self.reducer_count_prices),
            MRStep(mapper=self.mapper_make_prices_key,
                   reducer = self.reducer_output_custs)
        ]
    
    
    def mapper_get_prices(self,key,line):
        (custID, itemID, price) = line.split(',')
        yield custID, float(price)
    
    def reducer_count_prices(self, custID, price):
        yield '%04d'%int(custID), sum(price)
        
    def mapper_make_prices_key(self, custID, totprice):
        yield '%04.02f'%float(totprice), custID #MR does string sorting on keys
        
    def reducer_output_custs(self, totprice, custIDs):
        for custID in custIDs: #some customers can have same totprice and will be grouped my mapper, so we extract them here 
            yield totprice, custID

if __name__ == '__main__':
    Spendbycustomersorted.run()

#!python Spendbycustomersorted.py customer-orders.csv > ordertotalssorted.txt