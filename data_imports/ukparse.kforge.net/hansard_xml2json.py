import argparse
import urllib2
import datetime
import os
from dateutil.parser import parse as parse_date

from BeautifulSoup import BeautifulSoup

import json

parser = argparse.ArgumentParser(description='Import text data from UK parliment hansard.')
parser.add_argument('--date'  , dest='date_import', type=parse_date, default=datetime.datetime.now(), help='date to import from hansard website')
parser.add_argument('--output', dest='file_out'                                                     , help='the outputfile to write to')
args = parser.parse_args()


def gen_hansard_url(report_type, date, page=1):
    """
    Generate a Hansard URL for a date
    """
    if report_type not in ['lordspages', 'debates', 'westminhall']:
        raise Exception('invalid param %s' % report_type)
    
    url_format_dict = dict(
        report_type = report_type,
        year        = date.year    ,
        month       = date.month   ,
        day         = date.day     ,
        page        = chr(ord('a')+(page-1)),
    )
    
    hansard_url = "http://ukparse.kforge.net/parldata/scrapedxml/%(report_type)s/%(report_type)s%(year)04d-%(month)02d-%(day)02d%(page)s.xml" % url_format_dict
    return hansard_url

def get_hansard_page_xml(report_type, date, page):
    """
    Check to see if a local cached copy is avalable - if so - use local file - else get data from site
    """
    xml_data = ""
    
    def gen_cache_filename(date, page):
        return os.path.relpath("cache/%s %s.xml" % (date.strftime('%Y-%m-%d'), page))
    
    cached_filename = gen_cache_filename(date,page)
    if os.path.exists(cached_filename):
        xml_data = open(cached_filename, 'r').read()
        #print "return cache: %s" % cached_filename
    else:
        url = gen_hansard_url(report_type, date, page)
        try:
            #print "read live %s" % url
            xml_data = urllib2.urlopen(url, timeout=10).read()
        except urllib2.HTTPError as http_error:
            response = http_error.read()
        
        file = open(cached_filename, 'a')
        file.write(xml_data)
        file.close()
        
    return xml_data

def get_hansard_data(report_type, date):
    """
    Hansard dates may take multiple pages
    Attempt the pages from 1 to infinity, exit on http not found error
    """
    hansard_data = []
    
    page = 1
    xml_data = True
    while xml_data:
        source_url = gen_hansard_url     (report_type, date, page)
        xml_data   = get_hansard_page_xml(report_type, date, page)
        
        parse_hansard_xml(xml_data, hansard_data, source_url, date)
        
        page      += 1
    return hansard_data

def parse_hansard_xml(xml_data, hansard_data, source_url, date):
    major_heading = ''
    minor_heading = ''
    details       = ''

    soup = BeautifulSoup(xml_data)
    #print soup.prettify()

    try:
        iter(soup.publicwhip)
    except: return

    for tag in soup.publicwhip:
        try: name = tag.name
        except: continue
        
        if name == 'major-heading':
            major_heading = ''
            minor_heading = ''
            details       = ''
            major_heading = tag.text
        if name == 'minor-heading':
            minor_heading = tag.text
            
        if tag.get('nospeaker'):
            details += tag.text
            
        if tag.get('speakerid'):            
            hansard_data.append(
                {
                    'speakerid'  : tag.get('speakerid'  ),
                    'speakername': tag.get('speakername'),
                    'url'        : tag.get('url')        ,
                    'text'       : tag.text              ,
                    'context'    : ' '.join([major_heading, minor_heading, details]),
                    'timestamp'  : date.strftime('%Y-%m-%d')+' '+tag.get('time'),
                }
            )    
    

hansard_data = get_hansard_data('debates', args.date_import)

if args.file_out:
    file = open(args.file_out, 'w')
    file.write(json.dumps(hansard_data))
    file.close()
else:
    import pprint
    pprint.pprint(hansard_data)
