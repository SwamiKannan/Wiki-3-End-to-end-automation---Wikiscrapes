import xml.sax
import logging
import multiprocessing
from multiprocessing import Process
import bz2
import argparse
from threading import Thread
import time
import os
import json
from cleaner import Cleaner

logger = logging.getLogger(__name__)


# cleaner.py from https://github.com/CyberZHG Git repo: https://github.com/CyberZHG/wiki-dump-reader/blob/master/wiki_dump_reader/cleaner.py


class WikiReader(xml.sax.ContentHandler):
    def __init__(self, ns_filter, callback):
        super().__init__()

        self.filter = ns_filter

        self.read_stack = []
        self.read_text = ''
        self.read_title = ''
        self.read_namespace = ''
        self.read_category = ''

        self.status_count = 0
        self.callback = callback

    def startElement(self, tag_name, attributes):
        if tag_name == "ns":
            self.read_namespace = None

        elif tag_name == "page":
            self.read_title = None
            self.read_cat = None

        elif tag_name == "title":
            self.read_title = ""

        elif tag_name == "category":
            self.read_cat = ""

        elif tag_name == 'id':
            self.read_id = ''
        else:
            return

        self.read_stack.append(tag_name)

    def characters(self, content):
        if len(self.read_stack) == 0:
            return None

        if self.read_stack[-1] == "text":
            self.read_text += content

        if self.read_stack[-1] == "title":
            self.read_title += content

        if self.read_stack[-1] == "ns":
            self.read_namespace = int(content)

        if self.read_stack[-1] == 'id':
            self.read_id = int(content) - 1

    def endElement(self, tag_name):
        if len(self.read_stack) > 0:
            if tag_name == self.read_stack[-1]:
                del self.read_stack[-1]

        if self.filter(self.read_namespace):
            self.status_count += 1
            self.callback(
                (self.read_id, self.read_title.replace('Category:', '')))




def process_article(aq, fq, shutdown, cleaner, category):
    while not (shutdown and aq.empty()):
        id, doc = aq.get()
        text = cleaner.clean_text(doc)
        # text = text.encode('latin-1', 'ignore')
        fq.put(json.dumps({id: text}))
        category.append(text)


def write_out(fq, shutdown):
    while not (shutdown and fq.empty()):
        line = fq.get()
        out_file.write(line)


def display(aq, fq, reader):
    while True:
        print("Queue sizes: aq={0} fq={1}. Read: {2}".format(
            aq.qsize(),
            fq.qsize(),
            reader.status_count))
        time.sleep(1)


if __name__ == "__main__":
    shutdown = False
    # parser = ArgumentParser()
    # parser.add_argument("wiki", help="wiki dump file .xml.bz2")
    # parser.add_argument("out", help="final file .txt")
    # args = parser.parse_args()
    wiki = "..//langchain//Physics//reference_data//raw_data//wiki\enwiktionary-20230820-pages-articles-multistream.xml.bz2"
    out = "wiki_cat.txt"

    manager = multiprocessing.Manager()
    fq = manager.Queue(maxsize=2000)
    aq = manager.Queue(maxsize=2000)
    final_categories = []

    c = Cleaner()

    wiki = bz2.BZ2File(wiki)
    out_file = open(os.path.join(out), "w+")

    reader = WikiReader(lambda ns: ns == 14, aq.put)

    status = Thread(target=display, args=(aq, fq, reader))
    status.start()

    print('Starting processing....')
    processes = {}
    for i in range(15):
        processes[i] = Process(target=process_article,
                               args=(aq, fq, shutdown, c, final_categories))
        processes[i].start()

    for i in range(15):
        if processes[i].is_alive():
            print(f'Process {i} is alive')
        else:
            print(f'Process {i} is not alive')

    write_thread = Thread(target=write_out, args=(fq, shutdown))
    write_thread.start()

    xml.sax.parse(wiki, reader)

    shutdown = True

    with open('category_set.txt', 'w') as f:
        f.write('\n'.join(list(set(final_categories))))
