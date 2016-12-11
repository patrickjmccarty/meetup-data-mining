"""Meetup event downloader script.
Patrick McCarty - 2016-12-10

This script uses the Meetup.com API to request meetup events within a radius around a given latitude and longitude.
It uses the /2/open_events API function, which has the following limitations:
- it only returns events with 3 or more RSVPs.
- it only returns past events within the last 1 month of time.
- it only returns upcoming events within the next 3 months.

We further limit the upcoming events to the next 1 month.
Note that the latitude and longitude output is for the event venue if available, or the general group location if not.
"""

from __future__ import print_function
from __future__ import unicode_literals
import calendar
import datetime
import requests
import time
import codecs
import sys

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

# You can test queries to Meetup's API using the Console at: https://secure.meetup.com/meetup_api/console/

# API key, can be obtained at https://secure.meetup.com/meetup_api/key/
api_key= ""

# Geographic search parameters.
search_lat = "32.955294"
search_lon = "-117.100140"
radius_miles = 30

# How many times to attempt the same request, which help deal with frequent 500 Internal Server Error failures.
num_request_attempts = 3

# Currently, the maximum number of results you can request from meetup per response is 200.
per_page = 200

def eprint(*args, **kwargs):
    """Convenience function to print to stderr."""
    print(*args, file=sys.stderr, **kwargs)

def get_categories():
    """Gets the list of possible meetup categories."""
    url = "https://api.meetup.com/2/categories?&sign=true&page=200"
    response = requests.get(url, params={"key":api_key})
    data = response.json()
    if response.status_code != 200:
        eprint("Error code:", response.status_code, "Details:", data['details'])
        exit(response.status_code);

    return data['results']

def main():
    """Main routine for downloading event data to a file."""
    categories = get_categories()

    with codecs.open("events.csv", "w", "utf-8") as output_file:
        # Write column names to CSV file.
        output_file.write("group_category,group_age_days,group.name,group.id,lat,lon,event.waitlist_count,event.yes_rsvp_count,event_day_of_week,event_hour_of_day,event_datetime,event.name,event.id,event.status\n")

        # I had to query the categories separately like this because the results do not include the group category info.
        for category in categories:
            # I had to query past and upcoming events separately due to a bug in the meetup server that doesn't follow the API documentation.
            for event_status in ['past','upcoming']:
                # Meetup.com documentation here: https://www.meetup.com/meetup_api/docs/2/open_events/
                # Past meetups are limited by the server to 1 month ago.
                # We chose to limit upcoming results to 1 month from now using the time parameter.
                # Caveat: /2/open_events API only lists meetups that have 3 or more RSVPs!
                # Quirks: status is supposed to allow "past,upcoming", but this doesn't work. Must only specify one at a time or else you get 0 results.
                only = "group.created,group.name,group.id,group.group_lat,group.group_lon,venue.lat,venue.lon,waitlist_count,yes_rsvp_count,time,name,id,status"
                url = "https://api.meetup.com/2/open_events?&sign=false&lat=" + search_lat + "&lon=" + search_lon + "&radius=" + str(radius_miles) + \
                      "&limited_events=true&text_format=plain&time=,1m&status=" + event_status + "&page=" + str(per_page) + "&only=" + only

                offset = 0
                downloaded = 0
                total = 1
                while downloaded < total:
                    # Sometimes meetup's servers give error 500 for no apparent reason. In that case we wait briefly and retry.
                    retry = 0
                    while retry < num_request_attempts:
                        retry += 1
                        try:
                            response = requests.get(url, params={"category":category['id'], "key":api_key, "offset":offset})
                            # Internal Server Error responses don't contain JSON.
                            if response.status_code == 500:
                                eprint("Error code: 500", response.text)
                                time.sleep(10)
                            else:
                                data = response.json()
                                if response.status_code != 200:
                                    eprint("Error code:", response.status_code, "Details:", data['details'])
                                    exit(response.status_code);
                                break
                        except ValueError as err:
                            eprint("ValueError:", err)
                            eprint(sys.exc_info())
                        except:
                            eprint(sys.exc_info())
                    if retry >= num_request_attempts:
                        eprint("Retried the request the maximum of", num_request_attempts, "times! Giving up!")
                        exit(response.status_code);

                    offset += 1
                    count = data['meta']['count']
                    total = data['meta']['total_count']
                    downloaded += count
                    for event in data['results']:
                        group = event['group']
                        group_created_datetime = datetime.datetime.fromtimestamp(long(group['created']) / 1000)
                        group_age_days = (datetime.datetime.today() - group_created_datetime).days

                        event_time = datetime.datetime.fromtimestamp(long(event['time']) / 1000)
                        # weekday() gives the day index where 0 = Monday.
                        event_day_of_week = calendar.day_name[event_time.weekday()]
                        event_hour_of_day = event_time.hour
                        event_datetime = event_time.strftime("%Y-%m-%d %H:%M")

                        # The venue location is not always publicly available, so fall back to the general group location when necessary.
                        if 'venue' in event:
                            venue = event['venue']
                            lat = venue['lat']
                            lon = venue['lon']
                        else:
                            lat = group['group_lat']
                            lon = group['group_lon']

                        # Every event is supposed to have a status, but apparently the meetup server has a bug and sometimes omits it for upcoming events.
                        # The event.status can theoretically be one of: "cancelled", "upcoming", "past", "proposed", "suggested", "draft"
                        # but the /2/open_events API function only returns 'upcoming' and 'past' status, so this code isn't really needed.
                        if 'status' in event:
                            event_status2 = event['status']
                        else:
                            event_status2 = event_status

                        # Write the data we want to a file. Note that we replace commas in the free-form string fields with semicolons to preserve the CSV format.
                        output_file.write(",".join(map(unicode, [category['name'], group_age_days, group['name'].replace(",",";"), group['id'], lat, lon,
                                                                 event['waitlist_count'], event['yes_rsvp_count'], event_day_of_week, event_hour_of_day,
                                                                 event_datetime, event['name'].replace(",",";"), event['id'], event_status2])))
                        output_file.write("\n")

                    # Print info about the progress and rate limiting so we can keep an eye on it.
                    print(category['name'], "("+event_status+")", "Downloaded:", downloaded, "/", total, "RateLimit:", response.headers['X-RateLimit-Limit'], "Remaining:", response.headers['X-RateLimit-Remaining'], "Resets in:", response.headers['X-RateLimit-Reset'])

                    # Check the remaining number of requests allowed in the current rate limit window.
                    if response.headers['X-RateLimit-Remaining'] == 0:
                        eprint("Reached limit! Waiting", response.headers['X-RateLimit-Reset'], "seconds...")
                        # Wait the number of seconds until the current rate limit window resets.
                        time.sleep(response.headers['X-RateLimit-Reset'] + 0.1)
    print("DONE!")

if __name__=="__main__":
    main()
