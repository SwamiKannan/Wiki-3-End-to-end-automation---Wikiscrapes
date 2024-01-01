import xml.sax
import logging
import requests
import time
import os
import json
import re
from cleaner import clean_text
import settings

logger = logging.getLogger(__name__)
re_mode = 0
replacements = {'[[': '', ']]': '', '==': ''}


# cleaner.py from https://github.com/CyberZHG Git repo:
# https://github.com/CyberZHG/wiki-dump-reader/blob/master/wiki_dump_reader/cleaner.py


# Implementation of the WikiReader class from https://jamesthorne.com/blog/processing-wikipedia-in-a-couple-of-hours/
class WikiReader(xml.sax.ContentHandler):
    def __init__(self, ns_filter, callback, link):
        super().__init__()

        self.filter = ns_filter
        self.link = link
        self.read_stack = []
        self.read_text = ''
        self.read_title = ''
        self.read_namespace = ''

        self.status_count = 0
        self.callback = callback

    def startElement(self, tag_name, attributes):
        if tag_name == "ns":
            self.read_namespace = None

        elif tag_name == "page":
            self.read_text = None
            self.read_title = None

        elif tag_name == "title":
            self.read_title = ""

        elif tag_name == "text":
            self.read_text = ""

        else:
            return

        self.read_stack.append(tag_name)

    def endElement(self, tag_name):
        if len(self.read_stack) > 0:
            if tag_name == self.read_stack[-1]:
                del self.read_stack[-1]

        if self.filter(self.read_namespace):
            if tag_name == "page" and self.read_text is not None:
                self.status_count += 1
                self.callback((self.read_title, self.read_text, self.link))

    def characters(self, content):
        if len(self.read_stack) == 0:
            return None

        if self.read_stack[-1] == "text":
            self.read_text += content

        if self.read_stack[-1] == "title":
            self.read_title += content

        if self.read_stack[-1] == "ns":
            self.read_namespace = int(content)


def process_text(text):
    rep = dict((re.escape(k), v) for k, v in replacements.items())
    pattern = re.compile("|".join(rep.keys()))
    result_cat = re.findall(r"\[Category:(.*?)\]", text)
    text = pattern.sub(lambda m: rep[re.escape(m.group(0))], text)
    text = text.split('See also')[0]

    return text, result_cat


def process_article(ctq, c1tq, arg_shutdown):
    while not (arg_shutdown and ctq.empty()):
        page_title, doc, link = ctq.get()
        text = clean_text(doc)
        text, categories = process_text(text)
        if "REDIRECT ".upper() not in text:
            try:
                c1tq.put({"page": page_title, "sentences": text, 'categories': categories})
            except Exception as e:
                print(f'Exception while processing article {page_title}; Exception: {e}')


def clean_name(name):
    replace_list = [':', ' ', '/', ')', '(', '\\', '\/', '?', '*', '"', "'"]
    for item in replace_list:
        while item in name:
            name = name.replace(item, '_')
    return name


def display(epnq, xcq, ctq, c1tq, rxq, arg_shutdown):
    while not arg_shutdown:
        print(
            "Queue sizes: pages_queue={0} xml_queue={1} content_queue={2} cleaned_queue={3} raw_xml_queue={4} files "
            "created={5}".format(
                epnq.qsize(),
                xcq.qsize(),
                ctq.qsize(),
                c1tq.qsize(),
                rxq.qsize(),
                settings.count_files
            ))
        time.sleep(1)


def get_content(epnq, xcq, rxq, arg_shutdown):
    while not (arg_shutdown and epnq.empty()):
        if not epnq.empty():
            page_name, page_url = epnq.get()
            root_url = 'https://en.wikipedia.org/wiki/Special:Export/'
            link = root_url + page_name
            try:
                response = requests.get(link)
                if response.status_code == 200:
                    xcq.put((page_name, page_url, response.content))
                    rxq.put((page_name, response.content))
                elif response.status_code == 429:
                    print('Wikipedia overloaded with our request for pages. Pausing requests...')
                    epnq.put((page_name, page_url))
                    time.sleep(30)
                else:
                    print(f'{link} not available')
                    print(response.status_code)
            except Exception as e:
                print(f'Requests.get failed for {link}. Exception: {e}')
                epnq.put((page_name, page_url))


def extract_content(xcq, ctq, arg_shutdown):
    while not (arg_shutdown and xcq.empty()):
        if not xcq.empty():
            page_name, page_url, content = xcq.get()
            sample_reader = WikiReader(lambda ns: ns == 0, ctq.put, page_url)
            xml.sax.parseString(content, sample_reader)


def write_xml_data(rxq, data_path, arg_shutdown):
    while not (arg_shutdown and rxq.empty()):
        name, content = rxq.get()
        name = clean_name(name)
        if not os.path.exists(os.path.join(data_path, name + '.xml')):
            with open(os.path.join(data_path, name + '.xml'), 'wb') as f:
                f.write(content)


def write_out(c1tq, data_path, arg_shutdown):
    while not (arg_shutdown and c1tq.empty()):
        details = c1tq.get()
        outfile_name = details['page']
        name = clean_name(outfile_name)
        line = json.dumps(details, ensure_ascii=False)
        if not os.path.exists(os.path.join(data_path, name + '.json')):
            with open(os.path.join(data_path, name + '.json'), "w", encoding='utf-8') as f:
                f.write(line + '\n')
                settings.count_files += 1
