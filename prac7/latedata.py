import time
from pyflink.common import Types, WatermarkStrategy, Duration, Configuration
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows


# Tells Flink where the event time is: the 2nd item of each tuple
class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# These settings make Flink handle one event at a time,
# so 4 test events behave like a real slow stream
env.set_buffer_timeout(0)
env.get_config().set_auto_watermark_interval(100)
conf = Configuration()
conf.set_string("python.fn-execution.bundle.size", "1")
conf.set_string("python.fn-execution.bundle.time", "1")
conf.set_string("python.operator-chaining.enabled", "false")
env.configure(conf)

# (status, event time in milliseconds). "Order confirmed" arrives LAST
events = [
    ("Picked up", 1000),
    ("At restaurant", 8000),
    ("Delivered", 22000),
    ("Order confirmed", 3000),
]
data = env.from_collection(
    events, type_info=Types.TUPLE([Types.STRING(), Types.LONG()]))


# Wait 1 second between events, like a slow network
def slow(e):
    time.sleep(1)
    return e


data = data.map(slow, output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))

# WATERMARK: allow events to be up to 2 seconds out of order
watermarks = (WatermarkStrategy
              .for_bounded_out_of_orderness(Duration.of_seconds(2))
              .with_timestamp_assigner(EventTimeAssigner()))
stream = data.assign_timestamps_and_watermarks(watermarks)

# SIDE OUTPUT: a label for the pipe that catches late events
late_tag = OutputTag("late_events",
                     Types.TUPLE([Types.STRING(), Types.LONG()]))

# WINDOW: 10-second tumbling windows, late events go to late_tag
windowed = (stream
            .key_by(lambda event: "delivery", key_type=Types.STRING())
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .side_output_late_data(late_tag))

# Join the names of all events that fell in the same window
main = windowed.reduce(
    lambda a, b: (a[0] + " + " + b[0], max(a[1], b[1])),
    output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))

late = main.get_side_output(late_tag)

main.map(lambda e: "MAIN OUTPUT (window result): " + str(e),
         output_type=Types.STRING()).print()
late.map(lambda e: "SIDE OUTPUT (late event): " + str(e),
         output_type=Types.STRING()).print()

env.execute("Food Delivery Late Data")
