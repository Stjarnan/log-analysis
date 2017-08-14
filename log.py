#!/usr/bin/env python2.7
"""
    This program will create a text file
    Run some database questions
    Output info based on db questions into the text file
"""
import psycopg2


CONN = psycopg2.connect("dbname=news")
CUR = CONN.cursor()


QUESTION1 = "What are the most popular three articles of all time?"
QUESTION2 = "Who are the most popular article authors of all time?"
QUESTION3 = "On which days did more than 1% of requests lead to errors?"


CALL1 = ("SELECT articles.title, COUNT(articles.title) AS Views FROM " +
         "articles JOIN log ON log.path LIKE CONCAT('%', articles.slug)" +
         "GROUP BY articles.title ORDER BY Views DESC LIMIT 3;")

CALL2 = ("SELECT authors.name, COUNT(authors.id) AS Views FROM articles" +
         " JOIN log ON log.path LIKE CONCAT('%', articles.slug) JOIN " +
         "authors ON articles.author = authors.id GROUP BY " +
         "authors.name ORDER BY Views DESC;")


CALL3 = ("SELECT DATE(time), ROUND(SUM(case when status != '200 OK'" +
         " then 1 else 0 end) * 100  / COUNT(*))  AS percent FROM" +
         " log GROUP BY DATE HAVING SUM(case when status != '200 OK'" +
         " then 1 else 0 end) >= COUNT(*) / 100;")


def print_data(question, data_from_db, measure):
    """
        This function prints the data received from the db queries
        and presents this data in a textfile
    """
    log_file = open("log.txt", 'a')
    log_file.write("\n" + question + "\n")
    for data in data_from_db:
        log_file.write(str(data[0]) + " - " + str(data[1]) + measure + "\n")
    log_file.close()


def top_articles():
    """ This function returns the top articles from db queries """
    CUR.execute(CALL1)
    response = CUR.fetchall()
    return response


def top_authors():
    """ This function returns the top authors from db queries """
    CUR.execute(CALL2)
    response = CUR.fetchall()
    return response


def error_count():
    """ This function returns error-prone days from db queries """
    CUR.execute(CALL3)
    response = CUR.fetchall()
    return response


def db_queries():
    """ This function runs queries to the db to extract needed info """
    answer1 = top_articles()
    answer2 = top_authors()
    answer3 = error_count()

    print_data(QUESTION1, answer1, " Views")
    print_data(QUESTION2, answer2, " Views")
    print_data(QUESTION3, answer3, "% Errors")

db_queries()

CONN.commit()
CUR.close()
CONN.close()
