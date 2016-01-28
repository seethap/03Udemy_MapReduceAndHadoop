# max temperature at different locations
from mrjob.job import MRJob

class MRMaxTemperature(MRJob):
    
    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):
        (location, date, data_type, data, x, y, z, w) = line.split(',')
        if (data_type == 'TMAX'):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature

    def reducer(self, location, temps):
        yield location, max(temps)


if __name__ == '__main__':
    MRMaxTemperature.run()

# !python Max-Temperatures.py 1800.csv > maxtemps.txt