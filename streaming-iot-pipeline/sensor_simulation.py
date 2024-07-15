import datetime
import logging
import time

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# This function returns the first observation time
def peek_timestamp(ifp):
    pos = ifp.tell()
    line = ifp.readline()
    ifp.seek(pos)
    return get_timestamp(line)
     

# This function returns the observation time
def get_timestamp(line):
    # print(line)
    line = line.decode("utf-8")
    timestamp = line.split(",")[0]
    return datetime.datetime.strptime(timestamp, TIME_FORMAT)

# This function publishes events to the Pub/Sub topic
def publish(publisher, topic, events):
    num_obs = len(events)
    if num_obs > 0:
        logging.info(f"Publishing {num_obs} events from {get_timestamp(events[0])}")
        for event in events:
            publisher.publish(topic, event)

# This function simulate data as real event
def simulate(topic, publisher, ifp, first_obs_time, program_start_time, speed_factor):
    to_publish = list()

    def compute_sleep_time(obs_time):
        time_elapsed = (datetime.datetime.utcnow() - program_start_time).seconds
        sim_time_elapsed = ((obs_time - first_obs_time).days * 86400.0 + (obs_time - first_obs_time).seconds) / speed_factor

        to_sleep_secs = sim_time_elapsed - time_elapsed

        return to_sleep_secs

    for line in ifp:
        event = line
        obs_time = get_timestamp(line)

        if compute_sleep_time(obs_time) > 1:
            publish(publisher, topic, to_publish)
            to_publish = list()

            to_sleep_secs = compute_sleep_time(obs_time)

            if to_sleep_secs > 0:
                logging.info(f"Sleeping {to_sleep_secs} seconds")
                time.sleep(to_sleep_secs)
        to_publish.append(event)
    publish(publisher, topic, to_publish)
