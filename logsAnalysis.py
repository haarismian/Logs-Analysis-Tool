#!/usr/bin/env python3
import psycopg2


def topThreeArticles():
    connection = psycopg2.connect("dbname=news")
    cursor = connection.cursor()
    cursor.execute(
        """SELECT title, count(title) FROM log INNER JOIN articles ON log.path
         LIKE '%'|| articles.slug group by title order by count(title) desc
         limit 3;"""
    )
    response = cursor.fetchall()
    print("The top 3 articles are:")
    for row in response:
        print("\"" + str(row[0]) + "\" with " + str(row[1]) + " views")
    print("")
    cursor.close()
    connection.close()


def topFiveAuthors():
    connection = psycopg2.connect("dbname=news")
    cursor = connection.cursor()
    cursor.execute(
        """
            SELECT name, count(name) FROM log inner join articles ON log.path
            LIKE '%'|| articles.slug inner join authors on
            articles.author=authors.id group by name order by count(name) desc;
            """
    )
    response = cursor.fetchall()
    print("The top 5 authors are:")
    for row in response:
        print("\"" + str(row[0]) + "\" with " + str(row[1]) + " views")
    print("")
    cursor.close()
    connection.close()


def errorCheck():
    connection = psycopg2.connect("dbname=news")
    cursor = connection.cursor()
    cursor.execute(
        """
            SELECT * FROM(SELECT date_trunc('day',log.time) as day,
            ROUND(100*COUNT(case when status!='200 OK' then log.time end)/
            COUNT(case when status='200 OK' then log.time end),2) as percentage
            FROM log log group by day) as foo where percentage>1;
            """
    )
    response = cursor.fetchall()
    print("The following days had more than 1% load errors:")
    for row in response:
        print(str(row[0]) + " with " + str(row[1]) + "% errors")
    print("")
    cursor.close()
    connection.close()


topThreeArticles()
topFiveAuthors()
errorCheck()
