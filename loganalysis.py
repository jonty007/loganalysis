#!/usr/bin/env python
"""Main file which call internal functions show the required data"""

import psycopg2

# Name of the database on which analysis needs to be done
DBNAME = "news"


def show_highest_viewed_articles():
    """Function to fetch and print articles with maximum views"""
    database = psycopg2.connect(database=DBNAME)
    cursor = database.cursor()
    cursor.execute("select a.title as title, count(*) as views from log as"
                   " l inner join articles as a on substring(l.path from 10)"
                   " like a.slug group by title order by views desc limit 3;")
    rows = cursor.fetchall()
    database.close()
    print "List of top three articles:"
    for row in rows:
        print row[0] + "  ---  " + str(row[1])
    print


def show_highest_viewed_authors():
    """Function to fetch and print authors with maximum views"""
    database = psycopg2.connect(database=DBNAME)
    cursor = database.cursor()
    cursor.execute("select auth.name as name, count(*) as views from log as"
                   " l inner join articles as a on substring(l.path from 10)"
                   " like a.slug inner join authors as auth on a.author = "
                   "auth.id group by name order by views desc limit 3;")
    rows = cursor.fetchall()
    database.close()
    print "List of top three authors:"
    for row in rows:
        print row[0] + "  ---  " + str(row[1])
    print


def show_day_with_higherror_rate():
    """Function to print the days with error rate more than 1"""
    database = psycopg2.connect(database=DBNAME)
    cursor = database.cursor()
    cursor.execute("select total.date as date, ((error.errorcount * 100 ) /"
                   " total.totalcount::float) as errorrate from (select "
                   "date_trunc('day', time) as date , count(*) as totalcount"
                   " from log group by date) as total inner join (select "
                   "date_trunc('day', time) as date , count(*) as errorcount"
                   " from log where status like '%404%' group by date) as "
                   "error on total.date = error.date and ((error.errorcount"
                   " * 100 ) / total.totalcount::float) > 1;")
    rows = cursor.fetchall()
    database.close()
    print "List of days when error rate was more than 1 %"
    for row in rows:
        print"{date} --- {percentage} %".format(
            date=str(row[0].strftime('%B %d, %Y')),
            percentage=str(round(row[1], 2)))
    print
show_highest_viewed_articles()
show_highest_viewed_authors()
show_day_with_higherror_rate()
