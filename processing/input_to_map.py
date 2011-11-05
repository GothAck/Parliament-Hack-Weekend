#!/usr/bin/python

import psycopg2
import psycopg2.extras


def create_input(conn):
    print "*** Inventing input"
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS input")
    cursor.execute("CREATE TABLE input(time_said time with time zone default current_time, name text, body text, source text)")
    for line in file("input-test.txt"):
        person, text = line.strip().split(" ", 1)
        cursor.execute("INSERT INTO input(name, body) VALUES (%s, %s)", (person, text))


def create_processed_tables(conn):
    print "*** Creating blank tables for output"
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS person")
    cursor.execute("CREATE TABLE person(id serial primary key, canonical_name text not null)")

    cursor.execute("DROP TABLE IF EXISTS word")
    cursor.execute("CREATE TABLE word(id serial primary key, canonical_name text not null)")

    cursor.execute("DROP TABLE IF EXISTS person_alias")
    cursor.execute("CREATE TABLE person_alias(person_id integer references person(id), name text not null)")

    cursor.execute("DROP TABLE IF EXISTS person_word")
    cursor.execute("CREATE TABLE person_word(person_id integer references person(id), word_id integer references word(id), uses integer not null default 0)")

    cursor.execute("DROP TABLE IF EXISTS word_alias")
    cursor.execute("CREATE TABLE word_alias(word_id integer references word(id), name text not null)")

    cursor.execute("DROP TABLE IF EXISTS word_word")
    cursor.execute("CREATE TABLE word_word(word_id integer references word(id), pair_id integer references word(id), uses integer not null default 0)")


def get_person_id(conn, name):
    cursor = conn.cursor()
    cursor.execute("SELECT person_id FROM person_alias WHERE name ILIKE %s", (name, ))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute("INSERT INTO person(canonical_name) VALUES (%s) RETURNING id" , (name, ))
        person_id = cursor.fetchone()[0]
        cursor.execute("INSERT INTO person_alias(person_id, name) VALUES (%s, %s)", (person_id, name))
        print "person %s not found, created new with id #%d" % (name, person_id)
        return person_id


def get_word_id(conn, name):
    cursor = conn.cursor()
    cursor.execute("SELECT word_id FROM word_alias WHERE name ILIKE %s", (name, ))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute("INSERT INTO word(canonical_name) VALUES (%s) RETURNING id" , (name, ))
        word_id = cursor.fetchone()[0]
        cursor.execute("INSERT INTO word_alias(word_id, name) VALUES (%s, %s)", (word_id, name))
        print "word %s not found, created new with id #%d" % (name, word_id)
        return word_id


def add_person_word(conn, person_id, word_id):
    cursor = conn.cursor()
    cursor.execute("SELECT uses FROM person_word WHERE person_id=%s AND word_id=%s", (person_id, word_id))
    row = cursor.fetchone()
    if row:
        cursor.execute("UPDATE person_word SET uses = uses + 1 WHERE person_id=%s AND word_id=%s", (person_id, word_id))
    else:
        cursor.execute("INSERT INTO person_word (person_id, word_id, uses) VALUES (%s, %s, 1)", (person_id, word_id))


def process(conn):
    print "*** Processing"
    cursor = conn.cursor()
    cursor.execute("SELECT time_said, name, body FROM input")
    for time, name, body in cursor:
        person_id = get_person_id(conn, name)
        for word in body.split():
            word_id = get_word_id(conn, word)
            add_person_word(conn, person_id, word_id)
        print time, name, body


def print_results(conn):
    print "*** Results"
    cursor = conn.cursor()
    cursor.execute("SELECT id, canonical_name FROM person")
    for person_id, person_name in cursor.fetchall():
        cursor.execute("SELECT word_id, uses FROM person_word WHERE person_id = %s", (person_id, ))
        for word_id in cursor.fetchall():
            cursor.execute("SELECT canonical_name, uses FROM person_word JOIN word ON person_word.word_id = word.id WHERE person_id = %s", (person_id, ))
            for word_name, uses in cursor.fetchall():
                print person_id, person_name, word_name, uses


def main():
    conn_string = "host='localhost' dbname='shish' user='shish' password='shish'"
    conn = psycopg2.connect(conn_string)

    create_input(conn)
    create_processed_tables(conn)
    process(conn)
    print_results(conn)

#conn.commit()

if __name__ == "__main__":
    main()

