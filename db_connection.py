# -------------------------------------------------------------------------
# AUTHOR: Christian
# FILENAME: db_connection.py
# SPECIFICATION: This part of the program is the backend that deals with the database
# FOR: CS 4250- Assignment #2
# TIME SPENT: 5 hours
# -----------------------------------------------------------*/

# importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor


def connectDataBase():
    # Create a database connection object using psycopg2
    DB_NAME = "CPP"
    DB_USER = "postgres"
    DB_PASS = "1234"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    # try except statement in case it fails
    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")


# This function creates the database tables in PostgresSQL
def createTables(cur, conn):
    try:
        # Create category table
        sql = "create table category(id_cat integer not null, name text not null, " \
              "constraint category_pk primary key (id_cat))"

        cur.execute(sql)

        # create document table
        sql = "create table document(doc integer not null, text text not null, " \
              "title text not null, num_chars integer not null, date date not null, " \
              "id_cat integer not null, " \
              "constraint document_pk primary key (doc), " \
              "constraint document_cat_fk foreign key (id_cat) references category(id_cat))"

        cur.execute(sql)

        # create word table
        sql = "create table word(term text not null, num_chars integer not null, " \
              "constraint word_pk primary key (term))"

        cur.execute(sql)

        # create indecies table
        sql = "create table indecies(doc integer not null, term text not null, term_count integer not null, " \
              "constraint index_pk primary key (doc, term), " \
              "constraint index_pk_fk foreign key (doc) references document(doc)," \
              "constraint index_fk2 foreign key (term) references word(term))"

        cur.execute(sql)

        conn.commit()



    except:

        conn.rollback()

        print("There was a problem during the database creation or the database already exists.")


def createCategory(cur, catId, catName):
    # Insert a category in the database
    sql = "insert into category (id_cat, name) values (%s, %s)"
    recset = [catId, catName]

    cur.execute(sql, recset)


def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Get the category id based on the informed category name
    sql = "select id_cat from category where category.name = %s"
    recset = [docCat]

    cur.execute(sql, recset)
    docuId = cur.fetchone()

    # 2 Insert the document in the database. For num_chars, discard the spaces and
    # punctuation marks.
    count = 0

    for char in docText:
        if char == ' ':
            continue
        elif char == '.':
            continue
        elif char == '!':
            continue
        elif char == '?':
            continue
        elif char == ',':
            continue
        else:
            count = count + 1

    sql = "insert into document (doc, text, title, num_chars, date, id_cat) VALUES (%s, %s, %s, %s, %s, %s)"
    recset2 = [docId, docText, docTitle, count, docDate, docuId['id_cat']]

    cur.execute(sql, recset2)

    # 3 Update the potential new terms.
    newDoc = docText.lower().split()

    word = []

    for words in newDoc:
        words = words.replace(".", "")
        words = words.replace(",", "")
        words = words.replace("!", "")
        words = words.replace("?", "")
        word.append(words)

    # adding term and term count into database
    for term in word:

        count = 0
        for char in term:
            count = count + 1

        sql = "select term from word where term = %s"
        recset = [term]

        cur.execute(sql, recset)
        index = cur.fetchone()

        if index == None:
            sql = "insert into word (term, num_chars) values (%s, %s)"
            recset2 = [term, count]

            cur.execute(sql, recset2)

        else:
            continue

    # 4 Update the index

    # dictionary for term and count
    combination = {}
    holder = []

    # Adds term to index and checks to see if it is already in the database
    for term in word:
        wordcount = 0
        for index in word:
            if term == index:
                if index in holder:
                    wordcount = wordcount + 1
                else:
                    wordcount = wordcount + 1
                    holder.append(index)
            combination[term] = wordcount

    # Goes through the dictionary in order to input into the database
    for key in combination:
        term = key
        count = combination[key]

        sql = "insert into indecies (doc, term, term_count) values (%s, %s, %s)"
        recset = [docId, term, count]

        cur.execute(sql, recset)


def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms

    sql = "select term from indecies where doc = %s"
    recset = [docId]
    cur.execute(sql, recset)
    wordcheck = cur.fetchall()

    # deletes index from indecies table
    sql = "delete from indecies where doc = %s"
    recset = [docId]
    cur.execute(sql, recset)

    # 1.2 Check if there are no more occurrences of the term in another document.
    for word in wordcheck:
        sql = "select term from indecies where term = %s"
        recset = [word['term']]
        cur.execute(sql, recset)
        checker = cur.fetchone()

        #If none, delete from database
        if checker == None:
            sql = "delete from word where term = %s"
            recset2 = [word['term']]
            cur.execute(sql, recset2)

    # Delete the document from the database
    sql = "delete from document where doc = %s"
    recset3 = [docId]
    cur.execute(sql, recset3)


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    # delete document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)


def getIndex(cur):

    # create string for result
    indicies = ""

    # Query the database to return the documents where each term occurs with their count
    sql = "select document.title, indecies.term, indecies.term_count from document " \
          "inner join indecies on document.doc = indecies.doc"

    cur.execute(sql)

    recset = cur.fetchall()

    # Adds all combinations to indicies
    for term in recset:
        indicies += term['term'] + ":" + term['title'] + ":" + str(term['term_count']) + ", "

    # returns to driver
    return indicies
