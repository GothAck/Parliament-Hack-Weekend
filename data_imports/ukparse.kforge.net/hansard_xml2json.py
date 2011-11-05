import argparse
import urllib2
import os
import json
import datetime
from dateutil.parser import parse as parse_date
from BeautifulSoup import BeautifulSoup


# Constants --------------------------------------------------------------------
report_types = ['lordspages', 'debates', 'westminhall']
sources      = ['local_only', 'ukparse']

# Argument Parsing -------------------------------------------------------------

parser = argparse.ArgumentParser(description='Import text data from UK parliment hansard.')
parser.add_argument('-d', '--date'       , dest='date_import', type=parse_date           , default=datetime.datetime.now(), help='date to import from hansard website')
parser.add_argument('-o', '--output'     , dest='output'                                                                  , help='the outputfile to write to or postgress for direct postgress')
parser.add_argument('-r', '--report_type', dest='report_type', choices=report_types      , default='debates'              , help='report type to query from %s' % report_types)
parser.add_argument('-s'  '--source'     , dest='source'     , choices=sources           , default='local_only'           , help='source of data from %s' % sources)
args = parser.parse_args()



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
        
        file = open(cached_filename, 'a')
        file.write(xml_data)
        file.close()
    #else:
        #print "unable to find data for %s %s %s" % (report_type, date, page)
        
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


# XML Processing ---------------------------------------------------------------

def parse_hansard_xml(xml_data, hansard_data, source_url, date):
    major_heading = ''
    minor_heading = ''
    details       = ''

    soup = BeautifulSoup(xml_data)
    #print soup.prettify()

    try   : iter(soup.publicwhip)
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
    
# Main -------------------------------------------------------------------------

hansard_data = get_hansard_data(args.report_type, args.date_import)

if args.output:
    file = open(args.output, 'w')
    file.write(json.dumps(hansard_data))
    file.close()
else:
    import pprint
    pprint.pprint(hansard_data)
