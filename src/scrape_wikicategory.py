from multiprocessing import Process
import multiprocessing
import logging
from threading import Thread
from wiki_explore import process_article, write_out, display, get_content, extract_content, write_xml_data
from parse_utils import check_link_format, get_page_names
import argparse
import settings
import os
from file_utils import initiate_file_opens

args_parser = argparse.ArgumentParser()
args_parser.add_argument('parent_link', help='The parent category page from where the crawling will begin. Such urls '
                                             'should be in the format "https://en.wikipedia.org/wiki/Category:". Refer '
                                             'to the README.md for more details')
args_parser.add_argument('-o', '--output_dir', help='The output directory where the output will be stored.If not '
                                                    'provided, the current directory will be considered the output '
                                                    'directory')
args_parser.add_argument('-pl', '--max_page_limit', help='Max number of page names and links to be outputted. If both '
                                                         'max_page_limit and max_cat_limit are mentioned, then the '
                                                         'crawling will stop when the first limit is hit. If neither '
                                                         'are provided the crawling will continue till all sub '
                                                         'categories and pages are captured ')
args_parser.add_argument('-cl', '--max_cat_limit',
                         help='Max number of category names and links to be outputted.If both '
                              'max_page_limit and max_cat_limit are mentioned, then the '
                              'crawling will stop when the first limit is hit. If neither '
                              'are provided the crawling will continue till all sub '
                              'categories and pages are captured ')

args_parser.add_argument('-d', '--depth',
                         help='Max depth of branches that the crawler will go through i.e from parent to subcategory '
                              'before crawling is halted.e.g. if we want 15 levels of category crawling from the root '
                              'url, we say -d 15 or  --depth 15')

args = args_parser.parse_args()
url = args.parent_link
parent_url = check_link_format(url)
output_dir = args.output_dir if args.output_dir else None
mpl = int(args.max_page_limit) if args.max_page_limit else -1
mcl = int(args.max_cat_limit) if args.max_cat_limit else -1
depth = int(args.depth) if args.depth else None

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    shutdown = False
    manager = multiprocessing.Manager()
    settings.init()
    updated_output_dir = initiate_file_opens(output_dir, parent_url)
    XML_DATA_PATH = os.path.join(updated_output_dir, 'xml_files')
    PROCESSED_DATA_PATH = os.path.join(updated_output_dir, 'text_files')
    settings.MAXSIZE_EPNQ = 100000
    settings.MAXSIZE_EOQ = 5000
    settings.text_files = [x[:-4] for x in os.listdir(PROCESSED_DATA_PATH)] # Remove XML file extension from file name.

    extracted_page_name_queue = manager.Queue(maxsize=settings.MAXSIZE_EPNQ)
    xml_content_queue = manager.Queue(maxsize=settings.MAXSIZE_EOQ)
    content_text_queue = manager.Queue(maxsize=settings.MAXSIZE_EOQ)
    cleaned_text_queue = manager.Queue(maxsize=settings.MAXSIZE_EOQ)
    raw_xml_queue = manager.Queue(maxsize=settings.MAXSIZE_EOQ)

    status = Thread(target=display, args=(extracted_page_name_queue, xml_content_queue, content_text_queue,
                                          cleaned_text_queue, raw_xml_queue,shutdown))
    status.start()

    get_pages = {}  # Download the XML file from the net
    for i in range(12):
        get_pages[i] = Process(target=get_content,
                               args=(extracted_page_name_queue, xml_content_queue, raw_xml_queue, shutdown))
        get_pages[i].start()

    retrieval_processes = {}  # Threads to get the content from the xml file
    for i in range(1):
        retrieval_processes[i] = Process(target=extract_content, args=(xml_content_queue, content_text_queue, shutdown))
        retrieval_processes[i].start()

    processing_processes = {}  # Threads to clean the data
    for i in range(2):
        processing_processes[i] = Process(target=process_article,
                                          args=(content_text_queue, cleaned_text_queue, shutdown))
        processing_processes[i].start()

    # Threads to write data to disk
    writing_final_process = {}
    for i in range(5):
        writing_final_process[i] = Thread(target=write_out, args=(cleaned_text_queue, PROCESSED_DATA_PATH, shutdown))
        writing_final_process[i].start()

    # Threads to write xml to disk
    writing_xml_processes = {}
    for i in range(5):
        writing_xml_processes[i] = Thread(target=write_xml_data, args=(raw_xml_queue, XML_DATA_PATH, shutdown))
        writing_xml_processes[i].start()

    parent_url = check_link_format(url)
    get_page_names(url, parent_url, mcl, mpl, depth, extracted_page_name_queue)
    shutdown = True if [extracted_page_name_queue.qsize(), xml_content_queue.qsize(), content_text_queue.qsize(),
                        cleaned_text_queue.qsize(), raw_xml_queue.qsize()] == [0, 0, 0, 0, 0] else False
