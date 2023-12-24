import requests
from bs4 import BeautifulSoup
from file_utils import write_files
import time
from wiki_explore import clean_name
import settings


def check_link_format(link):
    """
    Most linked pages on wiki are of the format "/wiki/....". Hence, this function checks if the link is as per that
    format. If so, append 'https://en.wikipedia.org' to it
    :param link:
    :return: link in correct format for requests.get()
    """
    return 'https://en.wikipedia.org' + link if link.startswith('/') else link


def process_page(url, parent_url, epnq):
    """
    Process the entire page. Extract four different sets of data: 1. Extract all subcategory names and add it to
    category_list 2. Extract all subcategory links and add it to category_link_list 3. Extract all page names and add
    it to pages_list 4. Extract all page links and add it to pages_link_list
    :param epnq: Queue into which the name and the url is added for downstream processing
    :param url: URL of the page that is to be processed
    :param parent_url: The original URL from where the script was run. This is used because only the
    original url is of the format "en.wikipedia.org/wiki/....." while all other urls are of the form "/wiki/..."
    :return: category_list, category_link_list, pages_list, pages_link_list
    """
    done_list = set()
    url = check_link_format(url)
    subcat_section, pages_section, next_page_flag, url_retrieved = get_sections_and_next_flag(url)
    if subcat_section:
        category_list, category_link_list = get_categories(subcat_section)
    else:
        category_list, category_link_list = None, None
    if pages_section:
        pages_list, pages_link_list = get_pages(pages_section, epnq)
        if next_page_flag:
            category_list1, category_link_list1, pages_list1, pages_link_list1, done_list1 = get_next_page(
                pages_section, parent_url, epnq)
            if category_list and category_list1:
                category_list.update(category_list1)
                category_link_list.update(category_link_list1)
            elif not category_list and category_list1:
                category_list, category_link_list = set(), set()
                category_list.update(category_list1)
                category_link_list.update(category_link_list1)
            pages_list.update(pages_list1)
            pages_link_list.update(pages_link_list1)
            done_list.update(done_list1)
    else:
        pages_list, pages_link_list = None, None

    if url_retrieved:
        if url == parent_url:
            done_list.add(url)
        else:
            done_list.add(url.replace('https://en.wikipedia.org', ''))
    else:
        print('URL not retrieved: ', url)
    return category_list, category_link_list, pages_list, pages_link_list, done_list


def get_sections_and_next_flag(url):
    """
    Every url provided is a category page e.g. https://en.wikipedia.org/wiki/Category:Physics_stubs which has two
    required subsections: Sub-category for to go deeper down the category hierarchy and pages which are the main
    pages we need to download for analysis. This function extracts the 'div' that relates to the "Subcategories"
    section and the "Pages in category ..." sections. It also checks if the list of pages is paginated i.e. there are
    more webpages to list the pages within the category. Specifically, it checks if there is a "Next Page" link in
    the "Pages in the category..." section
    :param url: url of the category page
    :return: subcat_section - div pertaining to the subcategory section
    pages_section - div pertaining to the pages section
    next_page_flag - True if the list of pages in the page_section is paginated or not (multiple_pages)
    url_retrieved - True if the page we do not get any error while retrieving a page. If True, this page will be added
    to the done_list and no attempt will be made to download and scrape this page again. Note: 404 status codes pages
    will still return url_retrieved = True  since we do not want to retry extracting the page again.
    This flag is to specifically check for timeout errors
    """
    page = None
    url_retrieved = False
    try:
        response = requests.get(url)
        if response.status_code == 200:
            page = response.text
            url_retrieved = True
        elif response.status_code == 429:
            print('Overloaded response from Wikipedia. Pausing requests...')
            url_retrieved = False
            time.sleep(30)
        else:
            print(response.status_code)
            page = None
            url_retrieved = True
        if page:
            parser = BeautifulSoup(page, 'html.parser')
            subcat_section = parser.find('div', id='mw-subcategories')
            pages_section = parser.find('div', id='mw-pages')
            if pages_section:
                next_page_find = pages_section.find_all('a')
                next_page_flag = True if 1 in ['next page' in fn.contents[0] for fn in next_page_find] else False
            else:
                next_page_flag = False
            return subcat_section, pages_section, next_page_flag, url_retrieved
        else:
            return None, None, None, url_retrieved
    except Exception as e:
        print(url)
        print('Exception', e)
        return None, None, None, url_retrieved


def get_categories(subcat_section):
    """
    This function takes as input, the subcat_section from get_sections_and_next_flag(). We use this to extract two
    different sets of info:
    1. The name of the subcategories within the section
    2. The links of the subcategories within the section return updated category_list, category_link_list

    :param subcat_section: Subcategory section of the chosen page
    :return: list of category names,list of category links in the subcategory section of the page
    """
    category_list, category_link_list = set(), set()
    subcats = subcat_section.find_all('div', class_='CategoryTreeItem')
    for subcat in subcats:
        a = subcat.find('a')
        if a:
            category_list.add(a.contents[0])  # Add categories to be processed
            try:
                category_link_list.add(a['href'])
            except Exception as e:
                print('Adding link failed', a, 'Exception: ', e)
        else:
            print(subcat)
    return category_list, category_link_list


def get_pages(pages_section, epnq):
    """
    This function takes as input, the pages section from get_sections_and_next_flag(). We use this to extract all the
    page names and links in the pages section. We add these page names and links to page_list, page_link_list
    respectively and return the updated page_list and pages_link_list
    :param epnq: Queue into which the name and the url is added for downstream processing
    :param pages_section: pages_section from the get_sections_and_next_flag
    :return: list of pages under the category, ist of links of pages under the category
    """
    pages_list, pages_link_list = set(), set()
    pages = pages_section.find_all('li')
    for page in pages:
        page_name, page_url = page.find('a').contents[0], (page.find('a'))['href']
        pages_list.add(page_name)
        pages_link_list.add(page_url)
        page_name_clean = clean_name(page_name)
        if page_name_clean not in settings.text_files:
            epnq.put((str(page_name), page_url))

    return pages_list, pages_link_list


def get_next_page(pages_section, parent_url, epnq):
    """
    Extracts url of the "Next page" text. Runs process_page() for the next page and returns requisite categories,
    categories, page names and page links on the "Next Page: get_pages_and_next_flag() get_categories get_pages
    :param pages_section: Section of the main page which is titled "Pages in category <category>
    :param parent_url: parent_url is just the root url from where the code starts. This is added only to pass this
    argument to the process_page() function
    :param epnq: Queue for storing (page names and page urls) for downstream processing
    :param pages_section as obtained from get_sections_and_next_flag() parent_url: the original url from
    where the scraping started. This is required for calling the process_page() function
    :return:category_list, category_link_list, pages_list, pages_link_list of the "Next Page"
    """
    a_tags = pages_section.find_all('a')
    for a_tag in a_tags:
        if a_tag.contents[0] == 'next page':
            next_page_link = check_link_format(a_tag['href'])
            return process_page(next_page_link, parent_url, epnq)


def update_settings(child_cat, child_cat_links, child_page, child_page_links, child_done_links):
    if child_cat: settings.cat_names.update(child_cat)
    if child_cat_links: settings.cat_links.update(child_cat_links)
    if child_page: settings.page_names.update(child_page)
    if child_page_links: settings.page_links.update(child_page_links)
    if child_done_links: settings.done_links.update(child_done_links)


def process_all_pages(child_depth_links, max_category_limit, max_page_limit, parent_url, i, epnq):
    if epnq.qsize() > 0.8 * settings.MAXSIZE_EPNQ:  # to ensure that the epnq queue does not get jammed
        time.sleep(10)
    print('Depth:', i)
    file_limit = False
    child_cat_depth, child_cat_links_depth, child_page_depth, child_page_links_depth, child_done_links_depth = set(), \
        set(), set(), set(), set()
    for url_index in range(len(list(child_depth_links))):
        url = list(child_depth_links)[url_index]
        child_cat, child_cat_links, child_page, child_page_links, child_done_links = process_page(url, parent_url, epnq)
        if child_cat: child_cat_depth.update(child_cat)
        if child_cat_links: child_cat_links_depth.update(child_cat_links)
        if child_page: child_page_depth.update(child_page)
        if child_page_links: child_page_links_depth.update(child_page_links)
        if child_done_links: child_done_links_depth.update(child_done_links)
        time.sleep(i * 1.2)
    child_depth_links.update(child_cat_links_depth)
    child_depth_links -= child_done_links_depth
    update_settings(child_cat_depth, child_cat_links_depth, child_page_depth, child_page_links_depth,
                    child_done_links_depth)
    write_files()
    i += 1
    if i % 1000 == 0:
        print(
            f'{i}th scraping session: Category links collected: {len(settings.cat_links)}\tDone Links:'
            f'{len(settings.done_links)}')
    if max_category_limit > 0 or max_page_limit > 0:
        if 0 < max_category_limit <= len(settings.cat_names):
            print(
                f'Max category length of {max_category_limit} reached. Total category names extracted: '
                f'{len(settings.cat_names)}')
            file_limit = True
        if 0 < max_page_limit <= len(settings.page_names):
            print(
                f'Max category length of {max_page_limit} reached. Total category names extracted:'
                f' {len(settings.cat_names)}')
            file_limit = True
    time.sleep((i + 1) * 5)  # wait for the downstream processes to execute their queues the current depth level
    return file_limit, i


def process_depth_page(url, depth, parent_url, max_page_limit, max_category_limit, epnq):
    file_limit = False
    child_cat, child_cat_links, child_page, child_page_links, child_done_links = process_page(url, parent_url, epnq)
    if child_cat is None:
        print('Child cat is None')
    if child_cat_links is None:
        print('Child cat links is None')
    if child_page is None:
        print('child_page is None')
    if child_page_links is None:
        print('child_page_links is None')
    if child_done_links is None:
        print('child_done_links is None')
    update_settings(child_cat, child_cat_links, child_page, child_page_links, child_done_links)
    i = 0
    child_depth_links = (child_cat_links - child_done_links)
    while i < depth and len(settings.cat_links - settings.done_links) > 0 and not file_limit:
        file_limit, i = process_all_pages(child_depth_links, max_category_limit, max_page_limit,
                                          parent_url, i, epnq)
    write_files()
    return None


def process_no_depth_page(url, parent_url, max_page_limit, max_category_limit, epnq):
    file_limit = False
    child_cat, child_cat_links, child_page, child_page_links, child_done_links = process_page(url, parent_url, epnq)
    update_settings(child_cat, child_cat_links, child_page, child_page_links, child_done_links)
    i = 0
    child_depth_links = set()
    while len(settings.cat_links - settings.done_links) > 0 and not file_limit:
        file_limit, i = process_all_pages(child_depth_links, max_category_limit, max_page_limit, parent_url, i, epnq)
    write_files()
    return None


def get_page_names(url, parent_url, mpl, mcl, depth, epnq):
    if not depth:
        process_no_depth_page(url, parent_url, mpl, mcl, epnq)
    else:
        process_depth_page(parent_url, depth, parent_url, mpl, mcl, epnq)
    write_files()
    for f in [settings.fcn, settings.fcl, settings.fdl, settings.fpl, settings.fpn]:
        f.close()
