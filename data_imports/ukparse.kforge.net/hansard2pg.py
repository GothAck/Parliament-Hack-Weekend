#!/usr/bin/python

import argparse
import urllib2
import os
import json
import datetime
import psycopg2
import sys
from dateutil.parser import parse as parse_date
from BeautifulSoup import BeautifulSoup
from HTMLParser import HTMLParser


# Constants --------------------------------------------------------------------

report_types = ['lordspages', 'debates', 'westminhall']
sources      = ['local_only', 'ukparse']

def to_id(n):
    if n.isdigit():
        return int(n)
    return -1


# Get data from source ---------------------------------------------------------

def gen_hansard_url(report_type, date, page=1):
    """
    Generate a Hansard URL for a date
    """    
    url_format_dict = dict(
        report_type = report_type,
        year        = date.year    ,
        month       = date.month   ,
        day         = date.day     ,
        page        = page         ,
    )
    hansard_url = "http://ukparse.kforge.net/parldata/scrapedxml/%(report_type)s/%(report_type)s%(year)04d-%(month)02d-%(day)02d%(page)s.xml" % url_format_dict
    return hansard_url

def get_hansard_page_xml(report_type, date, page):
    """
    Check to see if a local cached copy is avalable - if so - use local file - else get data from site
    """
    print "Fetching data for %s:%s/%s" % (report_type, date, page)
    page = chr(ord('a')+(page-1)) # convert page number to letter
    xml_data = ""
    
    def gen_cache_filename(report_type, date, page):
        try   : os.makedirs('cache')
        except: pass
        return os.path.relpath(os.path.join('cache', "%s%s%s.xml" % (report_type, date.strftime('%Y-%m-%d'), page)))
    
    cached_filename = gen_cache_filename(report_type,date,page)
    
    if os.path.exists(cached_filename):
        xml_data = open(cached_filename, 'r').read()
        #print "return cache: %s" % cached_filename
    elif args.source == 'ukparse':
        url = gen_hansard_url(report_type, date, page)
        try:
            #print "read live %s" % url
            xml_data = urllib2.urlopen(url, timeout=10).read()
        except urllib2.HTTPError as http_error:
            response = http_error.read()
        #if xml_data:
        file = open(cached_filename, 'a')
        file.write(xml_data)
        file.close()
    #else:
        #print "unable to find data for %s %s %s" % (report_type, date, page)
        
    return xml_data

def get_hansard_data(report_type, date, hansard_data=None):
    """
    Hansard dates may take multiple pages
    Attempt the pages from 1 to infinity, exit on http not found error
    """
    if not isinstance(hansard_data, list):
        hansard_data = []
    
    page = 1
    xml_data = True
    while xml_data:
        source_url = gen_hansard_url     (report_type, date, page)
        xml_data   = get_hansard_page_xml(report_type, date, page)
        parse_hansard_xml(xml_data, source_url, date)
        page      += 1
    return hansard_data


# XML Processing ---------------------------------------------------------------

def parse_hansard_xml(xml_data, source_url, date, callback):
    major_heading = ''
    minor_heading = ''
    details       = ''

    soup = BeautifulSoup(xml_data)
    #print soup.prettify()

    try   : iter(soup.publicwhip)
    except: return

    for n, tag in enumerate(soup.publicwhip):
        #print "%s %d\r" % (date, n)

        try: name = tag.name
        except: continue
        
        if name == 'major-heading':
            major_heading = ''
            minor_heading = ''
            details       = ''
            major_heading = tag.text
            #print major_heading
        if name == 'minor-heading':
            minor_heading = tag.text
            
        if tag.get('nospeaker'):
            details += ' %s' % tag.text
            
        if tag.get('speakerid'):
            callback(
                {
                    'speakerid'  : to_id(tag.get('speakerid'  ).split("/")[-1]),
                    'speakername': tag.get('speakername'),
                    'url'        : tag.get('url')        ,
                    'text'       : HTMLParser.unescape.__func__(HTMLParser, tag.text),
                    'context'    : HTMLParser.unescape.__func__(HTMLParser, ' '.join([major_heading, minor_heading, details]) ),
                    'timestamp'  : date.strftime('%Y-%m-%d')+' '+tag.get('time'),
                }
            )
    print ""
    

# Main -------------------------------------------------------------------------

def main(args):
    # Argument Parsing -------------------------------------------------------------
    parser = argparse.ArgumentParser(description='Import text data from UK parliment hansard.')
    parser.add_argument('-d' , '--date'       , dest='date'       ,                             default=datetime.datetime.now(), help='date to import from hansard website')
    parser.add_argument('-dr', '--date_range' , dest='date_range' , nargs=2                                                    , help='date range to rip from')
    parser.add_argument('-o' , '--output'     , dest='file_out'                                                                , help='the outputfile to write to')
    parser.add_argument('-r' , '--report_type', dest='report_type', choices=report_types      , default='debates'              , help='report type to query from %s' % report_types)
    parser.add_argument('-s ', '--source'     , dest='source'     , choices=sources           , default='ukparse'              , help='source of data from %s' % sources)
    parser.add_argument(       '--db'         , dest='db_out'     , action="store_true"       , default=False                  , help='output to db')
    parser.add_argument(       '--clean'      , dest='clean'      , action="store_true"       , default=False                  , help='wipe / create blank DB first')
    args = parser.parse_args(args[1:])

    # Set up output streams --------------------------------------------------------
    file = None
    cursor = None
    conn = None

    if args.file_out:
        file = open(args.file_out, 'w')
    if args.db_out:
        conn_string = "host='localhost' dbname='%(user)s' user='%(user)s' password='%(user)s'" % {'user': os.environ['USER']}
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        if args.clean:
            cursor.execute("DROP TABLE IF EXISTS input CASCADE")
            cursor.execute("CREATE TABLE input(speakerid integer, speakername text, url text, text text, context text, timestamp text)")

    # Process data -----------------------------------------------------------------
    processor = None
    if args.file_out:
        def callback(data):
            file.write(json.dumps(data))
            file.write(",\n")
        processor = callback
    if args.db_out:
        def callback(data):
            cursor.execute(
                """
                    INSERT INTO input(
                        timestamp,
                        speakerid,
                        speakername,
                        text,
                        context,
                        url
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    data["timestamp"],
                    data['speakerid'],
                    data['speakername'],
                    data["text"],
                    data["context"],
                    data["url"],
                )
            )
        processor = callback

    if args.date:
        args.date_range = args.date, args.date
    if args.date_range:
        args.date_range = [parse_date(date) for date in args.date_range]
        day = datetime.timedelta(days=1)
        date = args.date_range[0]
        hansard_data = []
        while date <= args.date_range[1]:
            
            page = 1
            xml_data = True
            while xml_data:
                source_url = gen_hansard_url     (args.report_type, date, page)
                xml_data   = get_hansard_page_xml(args.report_type, date, page)
                parse_hansard_xml(xml_data, source_url, date, processor)
                page      += 1
            print date, page
            #print "%s - %d total data entries" % (date, len(hansard_data))
            date += day

    # Finish output streams --------------------------------------------------------
    if args.file_out:
        file.close()
    if args.db_out:
        conn.commit()
        cursor.execute("UPDATE input SET timestamp = overlay(timestamp placing '' from 12 for 1) WHERE length(timestamp) = 20")
        conn.commit()

    #else:
    #    import pprint
    #    pprint.pprint(hansard_data)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
