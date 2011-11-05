import argparse
import urllib2
import datetime
import os
from dateutil.parser import parse as parse_date

from BeautifulSoup import BeautifulSoup

parser = argparse.ArgumentParser(description='Import text data from UK parliment hansard.')
parser.add_argument('--date', dest='date_import', type=parse_date, default=datetime.datetime.now(), help='date to import from hansard website')
args = parser.parse_args()


def gen_hansard_url(date, page=1):
    """
    Generate a Hansard URL for a date
    
    Warning the cm201011, the parlementary year does not match the julian year.
    I dont know the cutoff date for the year change so this may break in future
    """
    url_format_dict = dict(
        prev_year  = date.year - 1,
        year_short = date.year % 2000,
        month      = date.month   ,
        day        = date.day     ,
        page       = page         ,
    )
    hansard_url = "http://www.publications.parliament.uk/pa/cm%(prev_year)04d%(year_short)02d/cmhansrd/cm%(year_short)02d%(month)02d%(day)02d/debtext/%(year_short)02d%(month)02d%(day)02d-%(page)04d.htm" % url_format_dict
    return hansard_url

def get_hansard_page_html(date, page):
    """
    Check to see if a local cached copy is avalable - if so - use local file - else get data from site
    """
    html_data = ""
    
    def gen_cache_filename(date, page):
        return os.path.relpath("%s %s.htm" % (date.strftime('%Y-%m-%d'), page))
    
    cached_filename = gen_cache_filename(date,page)
    if os.path.exists(cached_filename):
        html_data = open(cached_filename, 'r').read()
        #print "return cache: %s" % cached_filename
    else:
        url = gen_hansard_url(date, page)
        try:
            #print "read live %s" % url
            html_data = urllib2.urlopen(url, timeout=10).read()
        except urllib2.HTTPError as http_error:
            response = http_error.read()
        
        file = open(cached_filename, 'a')
        file.write(html_data)
        file.close()
        
    return html_data

def get_hansard_data(date):
    """
    Hansard dates may take multiple pages
    Attempt the pages from 1 to infinity, exit on http not found error
    """
    hansard_data = []
    
    page = 1
    html_data = True
    while html_data:
        source_url = gen_hansard_url      (date, page)
        html_data  = get_hansard_page_html(date, page)
        
        parse_hansard_html(html_data, hansard_data, source_url)
        
        page      += 1
    return hansard_data

def parse_hansard_html(html_data, hansard_data, source_url):
    soup = BeautifulSoup(html_data)
    
    
    #loose_content = soup.find('div', id='content-small')
    
    #quotes = soup.findAll(lambda tag: )
    
    #findPreviousSibling
    
    #for notus_date in soup.findAll('notus-date'):
    #    col = notus_date.find('a', {'class':"anchor-column"})
    #    if col:
    #        print col.get('name')
            
    
    #print soup.html.head.title.string
    #
    #    col = notus_date.find('a', {'class':"anchor-column"})
    #    if col:
    #        print col.get('name')
    #    #print a.get('href')
    
    

get_hansard_data(args.date_import)
