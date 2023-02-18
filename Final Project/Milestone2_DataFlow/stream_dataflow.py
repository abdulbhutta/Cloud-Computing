import argparse
import json
import logging
import os
import apache_beam as beam
import tensorflow as tf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class conversion(beam.DoFn):
    def process(self, element):
        conversionData=element
        celsiusTemp = float(conversionData['temperature'])
        pressureKPA = float(conversionData['pressure'])
        convertedTemp=  celsiusTemp * 1.8 + 32
        convertedPSI = pressureKPA / 6.895
        conversionData['temperature'] = str(convertedTemp)
        conversionData['pressure'] = str(convertedPSI)
        return [conversionData]

def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True;
  
    with beam.Pipeline(options=pipeline_options) as p:
        sensorData = (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input)
        | "toDict" >> beam.Map(lambda x: json.loads(x)));

        filteredData = sensorData | "Filtered Data" >> beam.Filter(lambda x: x['temperature'] is not None and x['humidity'] is not None and x['pressure'] is not None)
        sensorCleanData = filteredData | 'Clean Data' >> beam.ParDo(conversion())

        (sensorCleanData | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8'))
        |   'to Pub/sub' >> beam.io.WriteToPubSub(topic=known_args.output));

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()




  