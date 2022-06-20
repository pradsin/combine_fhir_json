import datetime


startDate = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(days=1)
endDate = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

if endDate < startDate:
    endDate = startDate + datetime.timedelta(days=7)

print(startDate, endDate);