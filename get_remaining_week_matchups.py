import pytz
from datetime import datetime, timedelta
from collections import defaultdict
import constants
import json

from utils.date_utils import *
from utils.nhl_api_utils import get_schedule

if __name__ == "__main__":
    today = str(datetime.now(pytz.UTC))
    year, week = get_fantasy_week(today)

    _, sunday_date = get_week_dates(year, week)
    print(sunday_date)

    today_as_date = today.split(" ")[0]

    start_date = datetime.strptime(today_as_date, "%Y-%m-%d")
    end_date = datetime.strptime(sunday_date, "%Y-%m-%d")
    dates_list = [
        (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date - start_date).days + 1)
    ]

    print(dates_list)

    sched_by_date = get_schedule_by_date(schedule_by_id=get_schedule())
    for date in dates_list:
        for game in sched_by_date[date]:
            print(game)
