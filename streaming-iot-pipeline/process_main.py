import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")

class LaneInfo(beam.DoFn):
    def process(self, element):
        yield self.new_lane_info(element)

    def new_lane_info(self, line):
        info = line.decode("utf-8").split(",") if not isinstance(line, str) else line.split(",")
        logging.info(f"Data Retrieved: {info}")
        return {
            "timestamp": info[0],
            "latitude": info[1],
            "longitude": info[2],
            "highway": info[3],
            "direction": info[4],
            "lane": info[5],
            "speed": float(info[-1]),
            "sensorId": ",".join(info[1:6])
        }


# Create a function to run pipeline
class AverageSpeedsPipeline:
    def __init__(self, laneInfo, options):
        self.laneInfo = laneInfo
        self.options = options

    def run(self):

        with beam.Pipeline(options=self.options) as p:
            averaging_interval = int(60 * (self.options.averaging_interval / self.options.speed_factor))
            averaging_frequency = int(averaging_interval / 2)
            schema = (
                    "timestamp:TIMESTAMP,"
                    "latitude:FLOAT,"
                    "longitude:FLOAT,"
                    "highway:STRING,"
                    "direction:STRING,"
                    "lane:INTEGER,"
                    "speed:FLOAT,"
                    "sensorId:STRING"
                )
            # Define the topic path
            topic = f"projects/{self.options.project}/topics/traffic"

            # Define the schema for your BigQuery table
            table_id = f"{self.options.project}:traffic.average_speed"

            # Read from Pubsub (streaming) and extract data
            current_condition = (
                p
                | 'GetMessages' >> beam.io.ReadFromPubSub(topic=topic)
                | 'ExtractData' >> beam.ParDo(self.laneInfo())
            )

            # Compute the average speed
            average_speed = (
                current_condition
                | 'TimeWindow' >> beam.WindowInto(beam.window.SlidingWindows(size=averaging_interval, period=averaging_frequency))
                | 'BySensor' >> beam.Map(lambda info: (info.get("sensorId"), info.get("speed")))
                | 'AvgBySensor' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
            )

            # Write transformed data to bigquery
            _ = (
                average_speed
                | 'ConvertData' >> beam.ParDo(self.to_bq_row)
                | 'ToBigquery' >> beam.io.WriteToBigQuery(
                    table=table_id,
                    schema=schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

    def to_bq_row(self, element):
        from datetime import datetime
        import time
        TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        logging.info(f"Element: {element}")

        sensor_key, average_speed = element
        line = f"{datetime.utcnow().strftime(TIME_FORMAT)},{sensor_key},{average_speed}"
        lane_info = self.laneInfo()
        info = lane_info.new_lane_info(line)
        
        logging.info(f"Loading data to BigQuery: {info}")
        time.sleep(5)
        yield info

class AverageSpeedOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--averaging_interval', 
            type=float, default=60.0, 
            help="How long to average (in minutes)"
            )
        
        parser.add_argument(
            '--speed_factor', 
            type=float, 
            default=60.0, 
            help="Simulation speed up factor"
            ) 

# Combine options into a single class
class CombinedPipelineOptions(AverageSpeedOptions, GoogleCloudOptions):
    pass

if __name__ == "__main__":
    # Define pipeline options
    options = PipelineOptions()
    combine_option = options.view_as(CombinedPipelineOptions)
    combine_option.project = "atlschool-demo"
    combine_option.averaging_interval = 60
    combine_option.speed_factor = 60
    
    pipeline = AverageSpeedsPipeline(laneInfo=LaneInfo, options=combine_option)
    pipeline.run()