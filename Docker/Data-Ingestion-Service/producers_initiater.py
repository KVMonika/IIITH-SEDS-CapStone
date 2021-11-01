import schedule
import time
from news_producer_1 import get_data_source_1, schedule_time_source_1
from news_producer_2 import get_data_source_2, schedule_time_source_2

# After every 10 mins get_data_source_1() is called.
schedule.every(schedule_time_source_1).minutes.do(get_data_source_1)

# After every 2 hours get_data_source_2() is called.
schedule.every(schedule_time_source_2).minutes.do(get_data_source_2)

get_data_source_1()
get_data_source_2()

while True:
    schedule.run_pending()
    time.sleep(1)

