import datetime as dt


def get_valid_stock_previous_day(dt_time):
    weekday = dt_time.weekday()
    if weekday == 0:
        previous = dt_time - dt.timedelta(3)
    elif weekday == 6:
        previous = dt_time - dt.timedelta(2)
    else:
        previous = dt_time - dt.timedelta(1)
    previous = dt.datetime.strftime(previous, '%Y%m%d')

    return previous


def nth_previous_day(n):
    try:
        previous = dt.date.today() - dt.timedelta(days=n)
    except Exception as e:
        print(e)

    return dt.date.strftime(previous, '%Y%m%d')
